"""Transactional monitoring snapshots and a bounded notification outbox."""

from __future__ import annotations

import json
import math
import re
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from sentinel_bot.config import IngressSettings, PushSource, SiteConfig
from sentinel_bot.events import (
    EventRejected,
    IncomingEvent,
    IncomingReceipt,
    OccurrenceState,
    decide_event,
    validate_event_time,
)
from sentinel_bot.models import (
    CheckResult,
    ErrorKind,
    Event,
    EventKind,
    Observation,
    PolicyState,
    SiteState,
)
from sentinel_bot.policy import apply_observation as decide_observation
from sentinel_bot.policy import due_reminder

SCHEMA_VERSION = 5
DATABASE_NAME = "state.sqlite3"
HISTORY_LIMIT = 200
MAX_PENDING_NOTIFICATIONS = 80
MAX_PENDING_PUSH_NOTIFICATIONS = 1000
MAX_IN_FLIGHT_NOTIFICATIONS = 20
MAX_IN_FLIGHT_EMAIL = 5
MAX_RETRY_DELAY_SECONDS = 7 * 86400


class StorageError(RuntimeError):
    """State cannot be used safely with the current schema."""


@dataclass(frozen=True)
class StartupMetadata:
    schema_version: int
    first_started_at: str
    last_started_at: str
    startup_count: int


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Time must be timezone-aware")
    return value.astimezone(UTC).isoformat()


@contextmanager
def _transaction(database: Path) -> Iterator[sqlite3.Connection]:
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(database, timeout=2.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except sqlite3.Error as exc:
        raise StorageError("Cannot commit SQLite state") from exc
    finally:
        if connection is not None:
            connection.close()


def _create_monitoring_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """CREATE TABLE sites (
            alias TEXT PRIMARY KEY,
            fingerprint TEXT NOT NULL,
            state TEXT NOT NULL,
            observation TEXT
        )"""
    )
    connection.execute(
        """CREATE TABLE notification_outbox (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            notification_id TEXT UNIQUE NOT NULL,
            source_alias TEXT NOT NULL,
            destination TEXT NOT NULL,
            incident_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            event TEXT NOT NULL,
            in_flight INTEGER NOT NULL DEFAULT 0 CHECK (in_flight IN (0, 1)),
            delivered_at TEXT
        )"""
    )
    connection.execute(
        """CREATE TABLE delivery_progress (
            source_alias TEXT NOT NULL,
            destination TEXT NOT NULL,
            incident_id TEXT NOT NULL,
            last_delivered_at TEXT NOT NULL,
            PRIMARY KEY (source_alias, destination)
        )"""
    )
    connection.execute(
        """CREATE TABLE incident_audit (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            source_alias TEXT NOT NULL,
            event TEXT NOT NULL
        )"""
    )
    connection.execute(
        """CREATE INDEX pending_notifications
        ON notification_outbox (source_alias, destination, delivered_at)"""
    )

    connection.execute(
        """CREATE UNIQUE INDEX one_pending_reminder
        ON notification_outbox (source_alias, destination, incident_id)
        WHERE kind = 'REMINDER' AND delivered_at IS NULL"""
    )


def _upgrade_schema(connection: sqlite3.Connection) -> None:
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    if version == 0:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
        if tables:
            raise StorageError("Existing database has an unversioned schema")
        connection.execute(
            """CREATE TABLE runtime_metadata (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                first_started_at TEXT NOT NULL,
                last_started_at TEXT NOT NULL,
                startup_count INTEGER NOT NULL CHECK (startup_count > 0)
            )"""
        )
    elif version not in (1, 2, 3, 4, SCHEMA_VERSION):
        raise StorageError(f"Unsupported state schema version {version}; expected {SCHEMA_VERSION}")
    if version == 1:
        row = connection.execute(
            "SELECT first_started_at, last_started_at, startup_count "
            "FROM runtime_metadata WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise StorageError("Startup metadata is missing")
    if version < 2:
        _create_monitoring_schema(connection)
    if version < 3:
        _create_delivery_schema(connection)
    if version < 4:
        _create_ingress_schema(connection)
    if version < 5:
        _create_email_schema(connection)
        connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    for query in (
        "SELECT alias, fingerprint, state, observation, delivery_destination, opening_id, "
        "opening_failed, reminder_failed FROM sites LIMIT 0",
        "SELECT sequence, notification_id, source_alias, destination, incident_id, "
        "kind, event, in_flight, delivered_at, attempts, next_attempt_at, attempted_at, "
        "failed_at, last_error, blocked_by, retry_delay_seconds FROM notification_outbox LIMIT 0",
        "SELECT source_alias, destination, incident_id, last_delivered_at "
        "FROM delivery_progress LIMIT 0",
        "SELECT sequence, source_alias, event FROM incident_audit LIMIT 0",
        "SELECT singleton, destination, pause_until, pause_delay_seconds, last_error "
        "FROM telegram_route LIMIT 0",
        "SELECT bot_id, next_offset, received_at FROM telegram_updates LIMIT 0",
        "SELECT source_alias, event_id, fingerprint, received_at, occurred_at, outcome "
        "FROM event_receipts LIMIT 0",
        "SELECT source_alias, incident_key, event_type, opened_at, resolved_at, retained_at, "
        "destination, opening_id, opening_failed FROM pushed_incidents LIMIT 0",
        "SELECT source_alias, received_at, occurred_at FROM push_sources LIMIT 0",
        "SELECT destination, enabled FROM email_routes LIMIT 0",
        "SELECT source_alias, destination, incident_id, opening_id, opening_failed, "
        "reminder_failed FROM email_site_routes LIMIT 0",
        "SELECT source_alias, incident_key, destination, opening_id, opening_failed "
        "FROM email_push_routes LIMIT 0",
        "SELECT sequence, notification_id, source_alias, destination, incident_id, kind, event, "
        "in_flight, delivered_at, attempts, next_attempt_at, attempted_at, failed_at, last_error, "
        "blocked_by, retry_delay_seconds FROM email_outbox LIMIT 0",
        "SELECT source_alias, destination, incident_id, last_delivered_at "
        "FROM email_progress LIMIT 0",
        "SELECT singleton, pause_until, pause_delay_seconds, last_error, suspended "
        "FROM email_channel LIMIT 0",
    ):
        connection.execute(query)
    if connection.execute("SELECT 1 FROM telegram_route WHERE singleton = 1").fetchone() is None:
        raise StorageError("Telegram route metadata is missing")
    if connection.execute("SELECT 1 FROM email_channel WHERE singleton = 1").fetchone() is None:
        raise StorageError("Email channel metadata is missing")


def _create_email_schema(connection: sqlite3.Connection) -> None:
    for statement in (
        """CREATE TABLE email_routes (
            destination TEXT PRIMARY KEY,
            enabled INTEGER NOT NULL CHECK (enabled IN (0, 1))
        )""",
        """CREATE TABLE email_channel (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            pause_until TEXT,
            pause_delay_seconds REAL,
            last_error TEXT,
            suspended INTEGER NOT NULL DEFAULT 0 CHECK (suspended IN (0, 1))
        )""",
        "INSERT INTO email_channel (singleton) VALUES (1)",
        """CREATE TABLE email_outbox (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            notification_id TEXT UNIQUE NOT NULL,
            source_alias TEXT NOT NULL,
            destination TEXT NOT NULL,
            incident_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            event TEXT NOT NULL,
            in_flight INTEGER NOT NULL DEFAULT 0 CHECK (in_flight IN (0, 1)),
            delivered_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT,
            attempted_at TEXT,
            failed_at TEXT,
            last_error TEXT,
            blocked_by TEXT,
            retry_delay_seconds REAL
        )""",
        "CREATE INDEX pending_email ON email_outbox (source_alias, destination, delivered_at)",
        """CREATE UNIQUE INDEX one_pending_email_reminder
        ON email_outbox (source_alias, destination, incident_id)
        WHERE kind = 'REMINDER' AND delivered_at IS NULL
        AND failed_at IS NULL AND blocked_by IS NULL""",
        """CREATE TABLE email_site_routes (
            source_alias TEXT NOT NULL,
            destination TEXT NOT NULL,
            incident_id TEXT NOT NULL,
            opening_id TEXT NOT NULL,
            opening_failed INTEGER NOT NULL DEFAULT 0,
            reminder_failed INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (source_alias, destination)
        )""",
        """CREATE TABLE email_push_routes (
            source_alias TEXT NOT NULL,
            incident_key TEXT NOT NULL,
            destination TEXT NOT NULL,
            opening_id TEXT NOT NULL,
            opening_failed INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (source_alias, incident_key, destination)
        )""",
        """CREATE TABLE email_progress (
            source_alias TEXT NOT NULL,
            destination TEXT NOT NULL,
            incident_id TEXT NOT NULL,
            last_delivered_at TEXT NOT NULL,
            PRIMARY KEY (source_alias, destination)
        )""",
    ):
        connection.execute(statement)


def _create_ingress_schema(connection: sqlite3.Connection) -> None:
    for statement in (
        """CREATE TABLE event_receipts (
            source_alias TEXT NOT NULL,
            event_id TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            received_at TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            outcome TEXT NOT NULL,
            PRIMARY KEY (source_alias, event_id)
        )""",
        "CREATE INDEX receipt_retention ON event_receipts (received_at)",
        """CREATE TABLE pushed_incidents (
            source_alias TEXT NOT NULL,
            incident_key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            opened_at TEXT,
            resolved_at TEXT,
            retained_at TEXT NOT NULL,
            destination TEXT NOT NULL,
            opening_id TEXT,
            opening_failed INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (source_alias, incident_key)
        )""",
        "CREATE INDEX tombstone_retention ON pushed_incidents (resolved_at, retained_at)",
        """CREATE TABLE push_sources (
            source_alias TEXT PRIMARY KEY,
            received_at TEXT,
            occurred_at TEXT
        )""",
    ):
        connection.execute(statement)


def _create_delivery_schema(connection: sqlite3.Connection) -> None:
    for statement in (
        "ALTER TABLE sites ADD COLUMN delivery_destination TEXT",
        "ALTER TABLE sites ADD COLUMN opening_id TEXT",
        "ALTER TABLE sites ADD COLUMN opening_failed INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE sites ADD COLUMN reminder_failed INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE notification_outbox ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE notification_outbox ADD COLUMN next_attempt_at TEXT",
        "ALTER TABLE notification_outbox ADD COLUMN attempted_at TEXT",
        "ALTER TABLE notification_outbox ADD COLUMN failed_at TEXT",
        "ALTER TABLE notification_outbox ADD COLUMN last_error TEXT",
        "ALTER TABLE notification_outbox ADD COLUMN blocked_by TEXT",
        "ALTER TABLE notification_outbox ADD COLUMN retry_delay_seconds REAL",
        "DROP INDEX one_pending_reminder",
        """CREATE UNIQUE INDEX one_pending_reminder
        ON notification_outbox (source_alias, destination, incident_id)
        WHERE kind = 'REMINDER' AND delivered_at IS NULL
        AND failed_at IS NULL AND blocked_by IS NULL""",
        """CREATE TABLE telegram_route (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            destination TEXT,
            pause_until TEXT,
            pause_delay_seconds REAL,
            last_error TEXT
        )""",
        "INSERT INTO telegram_route VALUES (1, NULL, NULL, NULL, NULL)",
        """CREATE TABLE telegram_updates (
            bot_id INTEGER PRIMARY KEY,
            next_offset INTEGER NOT NULL CHECK (next_offset >= 0),
            received_at TEXT NOT NULL
        )""",
    ):
        connection.execute(statement)
    connection.execute(
        """UPDATE sites SET
        delivery_destination = (SELECT destination FROM notification_outbox
            WHERE source_alias = sites.alias AND kind = 'DOWN'
            AND incident_id = json_extract(sites.state, '$.incident_id')
            ORDER BY sequence DESC LIMIT 1),
        opening_id = (SELECT notification_id FROM notification_outbox
            WHERE source_alias = sites.alias AND kind = 'DOWN'
            AND incident_id = json_extract(sites.state, '$.incident_id')
            ORDER BY sequence DESC LIMIT 1)"""
    )


def initialize_state(state_dir: Path, started_at: datetime) -> StartupMetadata:
    """Atomically upgrade supported state and commit one startup.

    The runtime holds its state-directory lock before calling this function.
    Run it outside the event loop; each call owns and closes its connection.
    """
    timestamp = _timestamp(started_at)
    try:
        with _transaction(state_dir / DATABASE_NAME) as connection:
            new_database = connection.execute("PRAGMA user_version").fetchone()[0] == 0
            _upgrade_schema(connection)
            if new_database:
                connection.execute(
                    "INSERT INTO runtime_metadata VALUES (1, ?, ?, 1)",
                    (timestamp, timestamp),
                )
            else:
                updated = connection.execute(
                    """UPDATE runtime_metadata
                    SET last_started_at = ?, startup_count = startup_count + 1
                    WHERE singleton = 1""",
                    (timestamp,),
                )
                if updated.rowcount != 1:
                    raise StorageError("Startup metadata is missing")
            row = connection.execute(
                """SELECT first_started_at, last_started_at, startup_count
                FROM runtime_metadata WHERE singleton = 1"""
            ).fetchone()
            if row is None:
                raise StorageError("Startup metadata is missing")
            return StartupMetadata(SCHEMA_VERSION, row[0], row[1], row[2])
    except StorageError as exc:
        if isinstance(exc.__cause__, sqlite3.Error):
            raise StorageError("Cannot initialize SQLite state") from exc.__cause__
        raise


@dataclass(frozen=True)
class SiteRecord:
    alias: str
    fingerprint: str
    state: PolicyState
    observation: Observation | None


@dataclass(frozen=True)
class OutboxRecord:
    id: str
    event: Event
    source_alias: str
    destination: str = "operator"
    in_flight: bool = False
    attempts: int = 0
    next_attempt_at: datetime | None = None
    attempted_at: datetime | None = None
    failed_at: datetime | None = None
    last_error: str | None = None
    blocked_by: str | None = None
    retry_delay_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class EmailDeliverySchedule:
    id: str
    destination: str
    in_flight: bool
    attempts: int
    retry_delay_seconds: float | None


@dataclass(frozen=True)
class ReminderRecord:
    source_alias: str
    destination: str
    incident_id: str
    last_delivered_at: datetime


def _encode_state(state: PolicyState) -> str:
    return json.dumps(
        {
            "confirmed": state.confirmed.value,
            "failures": state.failures,
            "successes": state.successes,
            "first_failure_at": _optional_timestamp(state.first_failure_at),
            "incident_id": state.incident_id,
            "incident_started_at": _optional_timestamp(state.incident_started_at),
        },
        separators=(",", ":"),
    )


def _encode_observation(observation: Observation) -> str:
    result = observation.result
    return json.dumps(
        {
            "checked_at": _timestamp(observation.checked_at),
            "success": result.success,
            "error": None if result.error is None else result.error.value,
            "http_status": result.http_status,
            "latency_ms": result.latency_ms,
        },
        separators=(",", ":"),
    )


def _encode_event(event: Event) -> str:
    document: dict[str, object] = {
        "kind": event.kind.value,
        "incident_id": event.incident_id,
        "occurred_at": _timestamp(event.occurred_at),
        "started_at": _timestamp(event.started_at),
    }
    if event.summary is not None:
        document.update(
            summary=event.summary,
            severity=event.severity,
            event_type=event.event_type,
            received_at=_optional_timestamp(event.received_at),
        )
    return json.dumps(document, separators=(",", ":"))


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _document(raw: str) -> dict[str, object]:
    document: object = json.loads(raw)
    if not isinstance(document, dict):
        raise ValueError("Stored value must be an object")
    return document


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Stored value must be a string")
    return value


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("Stored value must be an integer")
    return value


def _datetime(value: object) -> datetime:
    timestamp = datetime.fromisoformat(_text(value))
    _timestamp(timestamp)
    return timestamp.astimezone(UTC)


def _optional_datetime(value: object) -> datetime | None:
    return None if value is None else _datetime(value)


def _decode_state(raw: str) -> PolicyState:
    data = _document(raw)
    return PolicyState(
        confirmed=SiteState(_text(data["confirmed"])),
        failures=_integer(data["failures"]),
        successes=_integer(data["successes"]),
        first_failure_at=_optional_datetime(data["first_failure_at"]),
        incident_id=None if data["incident_id"] is None else _text(data["incident_id"]),
        incident_started_at=_optional_datetime(data["incident_started_at"]),
    )


def _decode_observation(raw: str) -> Observation:
    data = _document(raw)
    success = data["success"]
    if not isinstance(success, bool):
        raise ValueError("Stored success must be a boolean")
    latency = data["latency_ms"]
    if latency is not None and (isinstance(latency, bool) or not isinstance(latency, int | float)):
        raise ValueError("Stored latency must be numeric")
    return Observation(
        _datetime(data["checked_at"]),
        CheckResult(
            success,
            None if data["error"] is None else ErrorKind(_text(data["error"])),
            None if data["http_status"] is None else _integer(data["http_status"]),
            latency,
        ),
    )


def _decode_event(raw: str) -> Event:
    data = _document(raw)
    return Event(
        EventKind(_text(data["kind"])),
        _text(data["incident_id"]),
        _datetime(data["occurred_at"]),
        _datetime(data["started_at"]),
        None if data.get("summary") is None else _text(data["summary"]),
        None if data.get("severity") is None else _text(data["severity"]),
        None if data.get("event_type") is None else _text(data["event_type"]),
        _optional_datetime(data.get("received_at")),
    )


def _site_record(row: sqlite3.Row) -> SiteRecord:
    try:
        return SiteRecord(
            row["alias"],
            row["fingerprint"],
            _decode_state(row["state"]),
            None if row["observation"] is None else _decode_observation(row["observation"]),
        )
    except (ValueError, KeyError, TypeError) as exc:
        raise StorageError("Stored monitoring snapshot is invalid") from exc


def _outbox_record(row: sqlite3.Row) -> OutboxRecord:
    try:
        return OutboxRecord(
            row["notification_id"],
            _decode_event(row["event"]),
            row["source_alias"],
            row["destination"],
            bool(row["in_flight"]),
            row["attempts"],
            _optional_datetime(row["next_attempt_at"]),
            _optional_datetime(row["attempted_at"]),
            _optional_datetime(row["failed_at"]),
            row["last_error"],
            row["blocked_by"],
            row["retry_delay_seconds"],
        )
    except (ValueError, KeyError, TypeError) as exc:
        raise StorageError("Stored notification is invalid") from exc


def _bot_id(value: int) -> None:
    if type(value) is not int or not 0 < value < 2**63:
        raise ValueError("Telegram bot ID must be a positive integer")


def _safe_error(value: str) -> None:
    if value not in {
        "interrupted",
        "destination_changed",
        "opening_failed",
        "retries_exhausted",
        "rate_limit",
        "network",
        "timeout",
        "server_error",
        "bad_request",
        "forbidden",
        "unauthorized",
        "invalid_response",
        "conflict",
        "webhook",
        "tls",
        "quota",
        "rejected",
        "invalid_recipient",
        "configuration",
    }:
        raise ValueError("Delivery error must be a normalized category")


def _retry_delay(value: float) -> float:
    if (
        isinstance(value, bool)
        or not math.isfinite(value)
        or not 0 <= value <= MAX_RETRY_DELAY_SECONDS
    ):
        raise ValueError("Retry delay must be finite and between zero and seven days")
    return float(value)


class SQLiteStore:
    """Use short synchronous transactions, serialized off-loop by the runtime.

    Opening the store does not initialize state or increment startup metadata.
    Notification records retain their configured destination across retries.
    No storage method makes a network call or assumes provider acceptance.
    """

    def __init__(self, state_dir: Path) -> None:
        self.database = state_dir / DATABASE_NAME
        if not self.database.is_file():
            raise StorageError("State must be initialized before opening the store")

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with _transaction(self.database) as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version != SCHEMA_VERSION:
                raise StorageError(
                    f"Unsupported state schema version {version}; expected {SCHEMA_VERSION}"
                )
            yield connection

    def reconcile(self, sites: Sequence[SiteConfig]) -> None:
        """Reset changed sources and stale confirmations without fake recovery.

        Call once at startup, after acquiring state ownership. Confirmed state,
        incident identity, and the latest snapshot survive unchanged inventories;
        pending confirmation series restart because restored readings are stale.
        """
        enabled = tuple(site for site in sites if site.enabled)
        aliases = tuple(site.alias for site in enabled)
        if len(aliases) > 20 or len(aliases) != len(set(aliases)):
            raise ValueError("Source inventory requires at most 20 unique aliases")
        with self._transaction() as connection:
            existing = {
                row["alias"]: _site_record(row)
                for row in connection.execute("SELECT * FROM sites").fetchall()
            }
            for alias in existing.keys() - set(aliases):
                self._cancel_unsent(connection, alias)
                connection.execute("DELETE FROM sites WHERE alias = ?", (alias,))
            for site in enabled:
                previous = existing.get(site.alias)
                if previous is None or previous.fingerprint != site.fingerprint:
                    self._cancel_unsent(connection, site.alias)
                    connection.execute(
                        """INSERT INTO sites (alias, fingerprint, state, observation)
                        VALUES (?, ?, ?, NULL)
                        ON CONFLICT(alias) DO UPDATE SET fingerprint = excluded.fingerprint,
                        state = excluded.state, observation = NULL,
                        delivery_destination = NULL, opening_id = NULL,
                        opening_failed = 0, reminder_failed = 0""",
                        (site.alias, site.fingerprint, _encode_state(PolicyState())),
                    )
                else:
                    self._reset_pending(connection, previous)

    @staticmethod
    def _cancel_unsent(connection: sqlite3.Connection, alias: str) -> None:
        connection.execute("DELETE FROM delivery_progress WHERE source_alias = ?", (alias,))
        connection.execute("DELETE FROM email_progress WHERE source_alias = ?", (alias,))
        connection.execute("DELETE FROM email_site_routes WHERE source_alias = ?", (alias,))
        connection.execute(
            """DELETE FROM email_outbox WHERE source_alias = ?
            AND kind IN ('DOWN', 'RECOVERY', 'REMINDER') AND delivered_at IS NULL
            AND in_flight = 0 AND attempts = 0 AND failed_at IS NULL AND blocked_by IS NULL""",
            (alias,),
        )
        connection.execute(
            """DELETE FROM notification_outbox
            WHERE source_alias = ? AND delivered_at IS NULL AND in_flight = 0
            AND kind IN ('DOWN', 'RECOVERY', 'REMINDER')
            AND attempts = 0 AND failed_at IS NULL AND blocked_by IS NULL""",
            (alias,),
        )

    @staticmethod
    def _reset_pending(connection: sqlite3.Connection, record: SiteRecord) -> None:
        state = replace(record.state, failures=0, successes=0, first_failure_at=None)
        connection.execute(
            "UPDATE sites SET state = ? WHERE alias = ?", (_encode_state(state), record.alias)
        )

    def reset_pending(self, alias: str) -> None:
        """Discard confirmation samples separated by a stale polling gap."""
        with self._transaction() as connection:
            record = self._read_site(connection, alias)
            self._reset_pending(connection, record)

    @staticmethod
    def _read_site(connection: sqlite3.Connection, alias: str) -> SiteRecord:
        row = connection.execute("SELECT * FROM sites WHERE alias = ?", (alias,)).fetchone()
        if row is None:
            raise StorageError("Source is not present in reconciled state")
        return _site_record(row)

    def read_site(self, alias: str) -> SiteRecord:
        with self._transaction() as connection:
            return self._read_site(connection, alias)

    def read_sites(self) -> tuple[SiteRecord, ...]:
        with self._transaction() as connection:
            return tuple(
                _site_record(row)
                for row in connection.execute("SELECT * FROM sites ORDER BY alias").fetchall()
            )

    def apply_observation(self, site: SiteConfig, observation: Observation) -> SiteRecord:
        """Commit a fresh reading, policy transition, and outbox atomically."""
        with self._transaction() as connection:
            record = self._read_site(connection, site.alias)
            if record.fingerprint != site.fingerprint or not site.enabled:
                raise StorageError("Source configuration was not reconciled")
            if record.observation == observation:
                return record
            transition = decide_observation(
                site.alias,
                site.fingerprint,
                record.state,
                observation,
                failure_confirmation=site.failure_confirmation,
                recovery_confirmation=site.recovery_confirmation,
            )
            connection.execute(
                "UPDATE sites SET state = ?, observation = ? WHERE alias = ?",
                (_encode_state(transition.state), _encode_observation(observation), site.alias),
            )
            if transition.event is not None:
                self._queue_event(connection, site.alias, transition.event)
                connection.execute(
                    "INSERT INTO incident_audit (source_alias, event) VALUES (?, ?)",
                    (site.alias, _encode_event(transition.event)),
                )
                connection.execute(
                    """DELETE FROM incident_audit WHERE sequence NOT IN
                    (SELECT sequence FROM incident_audit ORDER BY sequence DESC LIMIT ?)""",
                    (HISTORY_LIMIT,),
                )
            return SiteRecord(site.alias, site.fingerprint, transition.state, observation)

    @staticmethod
    def _queue_event(connection: sqlite3.Connection, alias: str, event: Event) -> None:
        if event.kind in (EventKind.DOWN, EventKind.RECOVERY):
            SQLiteStore._queue_email_site_event(connection, alias, event)
        route = connection.execute(
            "SELECT destination FROM telegram_route WHERE singleton = 1"
        ).fetchone()
        destination = "operator" if route is None or route[0] is None else str(route[0])
        current = connection.execute(
            "SELECT delivery_destination, opening_id, opening_failed FROM sites WHERE alias = ?",
            (alias,),
        ).fetchone()
        if event.kind is EventKind.DOWN and (route is None or route[0] is None):
            if SQLiteStore._active_email_destinations(connection):
                connection.execute(
                    "UPDATE sites SET delivery_destination = 'disabled', opening_id = NULL, "
                    "opening_failed = 0, reminder_failed = 0 WHERE alias = ?",
                    (alias,),
                )
                return
        elif current is not None and current[0] == "disabled":
            return
        blocked_by = None
        if event.kind is not EventKind.DOWN and current is not None:
            destination = current[0] or destination
            if current[2]:
                blocked_by = current[1]
        unavailable_route = route is not None and route[0] is not None and destination != route[0]
        if event.kind is EventKind.DOWN:
            connection.execute("DELETE FROM delivery_progress WHERE source_alias = ?", (alias,))
            connection.execute(
                """DELETE FROM notification_outbox WHERE source_alias = ?
                AND destination = ? AND incident_id != ?
                AND kind IN ('DOWN', 'RECOVERY', 'REMINDER')
                AND delivered_at IS NULL AND in_flight = 0
                AND attempts = 0 AND failed_at IS NULL AND blocked_by IS NULL""",
                (alias, destination, event.incident_id),
            )
        elif event.kind is EventKind.RECOVERY:
            connection.execute(
                """DELETE FROM notification_outbox WHERE source_alias = ?
                AND destination = ? AND kind = 'REMINDER'
                AND delivered_at IS NULL AND in_flight = 0
                AND attempts = 0 AND failed_at IS NULL AND blocked_by IS NULL""",
                (alias, destination),
            )
        pending = connection.execute(
            "SELECT COUNT(*) FROM notification_outbox WHERE kind IN ('DOWN', 'RECOVERY', 'REMINDER') "
            "AND delivered_at IS NULL "
            "AND failed_at IS NULL AND blocked_by IS NULL"
        ).fetchone()[0]
        if blocked_by is None and not unavailable_route and pending >= MAX_PENDING_NOTIFICATIONS:
            raise StorageError("Notification outbox capacity is exhausted")
        notification_id = str(uuid4())
        connection.execute(
            """INSERT INTO notification_outbox
            (notification_id, source_alias, destination, incident_id, kind, event, blocked_by,
            failed_at, last_error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                notification_id,
                alias,
                destination,
                event.incident_id,
                event.kind.value,
                _encode_event(event),
                blocked_by,
                _timestamp(event.occurred_at) if unavailable_route and blocked_by is None else None,
                "destination_changed" if unavailable_route and blocked_by is None else None,
            ),
        )
        if event.kind is EventKind.DOWN:
            connection.execute(
                """UPDATE sites SET delivery_destination = ?, opening_id = ?,
                opening_failed = 0, reminder_failed = 0
                WHERE alias = ?""",
                (destination, notification_id, alias),
            )
        SQLiteStore._prune_history(connection)

    def reconcile_push_sources(self, sources: Sequence[PushSource]) -> None:
        """Bound the status cache without removing accepted events or occurrences."""
        aliases = tuple(source.alias for source in sources)
        if len(aliases) > 20 or len(set(aliases)) != len(aliases):
            raise ValueError("Push inventory requires at most 20 unique aliases")
        with self._transaction() as connection:
            for row in connection.execute("SELECT source_alias FROM push_sources").fetchall():
                if row[0] not in aliases:
                    connection.execute("DELETE FROM push_sources WHERE source_alias = ?", (row[0],))
            for alias in aliases:
                connection.execute(
                    "INSERT OR IGNORE INTO push_sources (source_alias) VALUES (?)", (alias,)
                )

    @staticmethod
    def _prune_event_retention(
        connection: sqlite3.Connection, now: datetime, retention: float
    ) -> None:
        cutoff = _timestamp(now - timedelta(seconds=retention))
        connection.execute("DELETE FROM event_receipts WHERE received_at < ?", (cutoff,))
        connection.execute(
            "DELETE FROM pushed_incidents WHERE resolved_at IS NOT NULL AND retained_at < ?",
            (cutoff,),
        )
        connection.execute(
            """DELETE FROM email_push_routes WHERE NOT EXISTS (
                SELECT 1 FROM pushed_incidents AS p WHERE
                p.source_alias = email_push_routes.source_alias
                AND p.incident_key = email_push_routes.incident_key)"""
        )

    @staticmethod
    def _event_capacity(
        connection: sqlite3.Connection,
        alias: str,
        limits: IngressSettings,
        *,
        active_delta: int,
        tombstone_delta: int,
        delivery_delta: int,
        telegram_delivery: bool = True,
    ) -> None:
        for source in (None, alias):
            condition = "" if source is None else " WHERE source_alias = ?"
            values = () if source is None else (source,)
            receipts = connection.execute(
                "SELECT COUNT(*) FROM event_receipts" + condition, values
            ).fetchone()[0]
            active, tombstones = connection.execute(
                "SELECT COUNT(*) FILTER (WHERE resolved_at IS NULL), "
                "COUNT(*) FILTER (WHERE resolved_at IS NOT NULL) FROM pushed_incidents" + condition,
                values,
            ).fetchone()
            pending = connection.execute(
                "SELECT COUNT(*) FROM notification_outbox "
                "WHERE kind IN ('NOTIFICATION', 'FIRING', 'RESOLVED') "
                "AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL"
                + ("" if source is None else " AND source_alias = ?"),
                values,
            ).fetchone()[0]
            telegram_active = connection.execute(
                "SELECT COUNT(*) FROM pushed_incidents WHERE resolved_at IS NULL "
                "AND destination != 'disabled'"
                + ("" if source is None else " AND source_alias = ?"),
                values,
            ).fetchone()[0]
            maxima = (
                (
                    limits.max_receipts,
                    limits.max_tombstones,
                    limits.max_pending,
                    limits.max_active_incidents,
                )
                if source is None
                else (
                    limits.max_receipts_per_source,
                    limits.max_tombstones_per_source,
                    limits.max_pending_per_source,
                    limits.max_active_incidents_per_source,
                )
            )
            # Every active occurrence owns a future resolution receipt, tombstone
            # and delivery slot; one-shots must not spend those reservations.
            if any(
                count > maximum
                for count, maximum in zip(
                    (
                        receipts + 1 + active + active_delta,
                        tombstones + tombstone_delta + active + active_delta,
                        pending
                        + int(telegram_delivery) * delivery_delta
                        + telegram_active
                        + int(telegram_delivery) * active_delta,
                        active + active_delta,
                    ),
                    maxima,
                    strict=True,
                )
            ):
                raise EventRejected("capacity")

    def accept_event(
        self,
        source: PushSource,
        event: IncomingEvent,
        received_at: datetime,
        limits: IngressSettings,
    ) -> IncomingReceipt:
        """Commit one immutable receipt, lifecycle decision and fixed-route delivery.

        Retained duplicates precede new-event permission, time and capacity checks.
        A returned receipt describes durable acceptance, never provider delivery.
        """
        timestamp = _timestamp(received_at)
        with self._transaction() as connection:
            previous_receipt = connection.execute(
                "SELECT * FROM event_receipts WHERE source_alias = ? AND event_id = ?",
                (source.alias, event.event_id),
            ).fetchone()
            if previous_receipt is not None:
                if previous_receipt["fingerprint"] != event.fingerprint:
                    raise EventRejected("conflict")
                return IncomingReceipt(
                    event.event_id,
                    _datetime(previous_receipt["received_at"]),
                    previous_receipt["outcome"],
                )
            if event.event_type not in source.allowed_types:
                raise EventRejected("event_type")
            validate_event_time(event, received_at, limits.max_event_age, limits.future_skew)
            self._prune_event_retention(connection, received_at, limits.retention)
            occurrence = None
            if event.incident_key is not None:
                occurrence = connection.execute(
                    "SELECT * FROM pushed_incidents WHERE source_alias = ? AND incident_key = ?",
                    (source.alias, event.incident_key),
                ).fetchone()
            previous = (
                None
                if occurrence is None
                else OccurrenceState(
                    occurrence["event_type"],
                    _optional_datetime(occurrence["opened_at"]),
                    _optional_datetime(occurrence["resolved_at"]),
                )
            )
            decision = decide_event(event, previous)
            closing_active = (
                previous is not None
                and previous.resolved_at is None
                and decision.state is not None
                and decision.state.resolved_at is not None
            )
            opening = previous is None and event.kind == "firing"
            tombstone = decision.state is not None and decision.state.resolved_at is not None
            route = connection.execute(
                "SELECT destination FROM telegram_route WHERE singleton = 1"
            ).fetchone()
            current_destination = None if route is None else route[0]
            email_destinations = self._active_email_destinations(connection)
            destination = (
                occurrence["destination"]
                if occurrence is not None
                else current_destination or ("disabled" if email_destinations else "operator")
            )
            if not closing_active:
                self._event_capacity(
                    connection,
                    source.alias,
                    limits,
                    active_delta=int(opening),
                    tombstone_delta=int(tombstone and previous is None),
                    delivery_delta=int(decision.notify),
                    telegram_delivery=destination != "disabled",
                )
                self._email_event_capacity(
                    connection,
                    source.alias,
                    limits,
                    email_destinations,
                    active_delta=int(opening),
                    delivery_delta=int(decision.notify),
                )
            notification_id = (
                str(uuid4()) if decision.notify and destination != "disabled" else None
            )
            if decision.state is not None:
                state = decision.state
                connection.execute(
                    """INSERT INTO pushed_incidents
                    (source_alias, incident_key, event_type, opened_at, resolved_at,
                    retained_at, destination, opening_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_alias, incident_key) DO UPDATE SET
                    resolved_at = excluded.resolved_at,
                    retained_at = CASE WHEN pushed_incidents.resolved_at IS NULL
                        AND excluded.resolved_at IS NOT NULL THEN excluded.retained_at
                        ELSE pushed_incidents.retained_at END""",
                    (
                        source.alias,
                        event.incident_key,
                        state.event_type,
                        _optional_timestamp(state.opened_at),
                        _optional_timestamp(state.resolved_at),
                        timestamp,
                        destination,
                        notification_id if opening else None,
                    ),
                )
            if decision.notify:
                outgoing = Event(
                    EventKind(event.kind.upper()),
                    event.incident_key or event.event_id,
                    event.occurred_at,
                    event.occurred_at
                    if decision.state is None or decision.state.opened_at is None
                    else decision.state.opened_at,
                    event.summary,
                    event.severity,
                    event.event_type,
                    received_at,
                )
                self._queue_email_push_event(connection, source.alias, outgoing, opening)
            if notification_id is not None:
                blocked_by = (
                    occurrence["opening_id"]
                    if closing_active and occurrence is not None and occurrence["opening_failed"]
                    else None
                )
                unavailable = current_destination is not None and destination != current_destination
                connection.execute(
                    """INSERT INTO notification_outbox
                    (notification_id, source_alias, destination, incident_id, kind, event,
                    blocked_by, failed_at, last_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        notification_id,
                        source.alias,
                        destination,
                        outgoing.incident_id,
                        outgoing.kind.value,
                        _encode_event(outgoing),
                        blocked_by,
                        timestamp if unavailable and blocked_by is None else None,
                        "opening_failed"
                        if blocked_by is not None
                        else "destination_changed"
                        if unavailable
                        else None,
                    ),
                )
            connection.execute(
                "INSERT INTO event_receipts VALUES (?, ?, ?, ?, ?, ?)",
                (
                    source.alias,
                    event.event_id,
                    event.fingerprint,
                    timestamp,
                    _timestamp(event.occurred_at),
                    decision.outcome,
                ),
            )
            connection.execute(
                """INSERT INTO push_sources VALUES (?, ?, ?)
                ON CONFLICT(source_alias) DO UPDATE SET received_at = excluded.received_at,
                occurred_at = excluded.occurred_at""",
                (source.alias, timestamp, _timestamp(event.occurred_at)),
            )
            self._prune_history(connection)
            return IncomingReceipt(event.event_id, received_at.astimezone(UTC), decision.outcome)

    def incoming_status(self, aliases: Sequence[str]) -> tuple[dict[str, object], ...]:
        """Read cached push activity without treating silence as healthy."""
        if len(aliases) > 20 or len(set(aliases)) != len(aliases):
            raise ValueError("Push status requires at most 20 unique aliases")
        with self._transaction() as connection:
            result: list[dict[str, object]] = []
            for alias in aliases:
                last = connection.execute(
                    "SELECT received_at, occurred_at FROM push_sources WHERE source_alias = ?",
                    (alias,),
                ).fetchone()
                active = connection.execute(
                    "SELECT COUNT(*) FROM pushed_incidents WHERE source_alias = ? "
                    "AND resolved_at IS NULL",
                    (alias,),
                ).fetchone()[0]
                pending = connection.execute(
                    "SELECT COUNT(*) FROM notification_outbox WHERE source_alias = ? "
                    "AND kind IN ('NOTIFICATION', 'FIRING', 'RESOLVED') "
                    "AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL",
                    (alias,),
                ).fetchone()[0]
                pending += connection.execute(
                    "SELECT COUNT(*) FROM email_outbox WHERE source_alias = ? "
                    "AND kind IN ('NOTIFICATION', 'FIRING', 'RESOLVED') "
                    "AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL",
                    (alias,),
                ).fetchone()[0]
                result.append(
                    {
                        "alias": alias,
                        "last_received_at": None if last is None else last[0],
                        "last_event_at": None if last is None else last[1],
                        "active_incidents": active,
                        "pending_notifications": pending,
                    }
                )
            return tuple(result)

    def pending_notifications(self) -> tuple[OutboxRecord, ...]:
        with self._transaction() as connection:
            return tuple(
                _outbox_record(row)
                for row in connection.execute(
                    """SELECT * FROM notification_outbox
                    WHERE delivered_at IS NULL AND failed_at IS NULL
                    AND blocked_by IS NULL ORDER BY sequence"""
                ).fetchall()
            )

    def pending_count(self) -> int:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT (SELECT COUNT(*) FROM notification_outbox WHERE delivered_at IS NULL "
                "AND failed_at IS NULL AND blocked_by IS NULL) + "
                "(SELECT COUNT(*) FROM email_outbox WHERE delivered_at IS NULL "
                "AND failed_at IS NULL AND blocked_by IS NULL)"
            ).fetchone()
            assert row is not None
            return int(row[0])

    def configure_telegram(self, destination: str | None, now: datetime) -> int:
        """Bind unsent logical records and recover interrupted sends at startup.

        Returns the count of interrupted sends. Their provider outcome is unknown;
        retrying their unchanged identity/destination can duplicate an accepted send.
        Disabling Telegram pauses its queue. Changing its destination visibly fails
        old pending records rather than disclosing them to the new destination.
        """
        timestamp = _timestamp(now)
        if (
            destination is not None
            and re.fullmatch(r"telegram:-?[1-9][0-9]*:[0-9]+", destination) is None
        ):
            raise ValueError("Telegram destination must contain fixed numeric chat/topic IDs")
        with self._transaction() as connection:
            interrupted = connection.execute(
                "SELECT COUNT(*) FROM notification_outbox WHERE in_flight = 1 "
                "AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL"
            ).fetchone()[0]
            if destination is not None:
                connection.execute(
                    """UPDATE notification_outbox SET destination = ?
                    WHERE destination = 'operator' AND delivered_at IS NULL
                    AND failed_at IS NULL AND blocked_by IS NULL
                    AND in_flight = 0 AND attempts = 0""",
                    (destination,),
                )
                connection.execute(
                    """UPDATE sites SET delivery_destination = ?
                    WHERE delivery_destination = 'operator' AND EXISTS
                    (SELECT 1 FROM notification_outbox WHERE notification_id = sites.opening_id
                        AND destination = ?)""",
                    (destination, destination),
                )
                connection.execute(
                    """UPDATE pushed_incidents SET destination = ?
                    WHERE destination = 'operator' AND EXISTS
                    (SELECT 1 FROM notification_outbox
                    WHERE notification_id = pushed_incidents.opening_id AND destination = ?)""",
                    (destination, destination),
                )
            connection.execute(
                """UPDATE notification_outbox SET in_flight = 0,
                attempts = MAX(attempts, 1), next_attempt_at = ?, last_error = 'interrupted'
                WHERE in_flight = 1 AND delivered_at IS NULL
                AND failed_at IS NULL AND blocked_by IS NULL""",
                (timestamp,),
            )
            connection.execute(
                "UPDATE telegram_route SET destination = ? WHERE singleton = 1", (destination,)
            )
            if destination is not None:
                rows = connection.execute(
                    """SELECT * FROM notification_outbox
                    WHERE destination LIKE 'telegram:%' AND destination != ?
                    AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL
                    ORDER BY sequence""",
                    (destination,),
                ).fetchall()
                for row in rows:
                    self._fail(connection, row, timestamp, "destination_changed")
                self._coalesce_pending(connection, destination)
            self._prune_history(connection)
            return int(interrupted)

    @staticmethod
    def _coalesce_pending(connection: sqlite3.Connection, destination: str) -> None:
        connection.execute(
            """DELETE FROM notification_outbox AS old
            WHERE old.destination = ? AND old.delivered_at IS NULL
            AND old.kind IN ('DOWN', 'RECOVERY', 'REMINDER')
            AND old.failed_at IS NULL AND old.blocked_by IS NULL
            AND old.in_flight = 0 AND old.attempts = 0
            AND EXISTS (SELECT 1 FROM notification_outbox AS latest
                WHERE latest.source_alias = old.source_alias
                AND latest.destination = old.destination AND latest.kind = 'DOWN'
                AND latest.sequence = (SELECT MAX(sequence) FROM notification_outbox
                    WHERE source_alias = old.source_alias AND destination = old.destination
                    AND kind = 'DOWN')
                AND latest.incident_id != old.incident_id)""",
            (destination,),
        )

    def reserve_due_delivery(
        self, now: datetime, *, ready_ids: Sequence[str] | None = None
    ) -> OutboxRecord | None:
        """Reserve one ordered send and persist its attempt before network I/O.

        A runtime may supply IDs whose in-memory monotonic retry deadline elapsed;
        otherwise persisted UTC deadlines are used. Neither path bypasses routing,
        terminal state, in-flight capacity, or DOWN-before-RECOVERY ordering.
        """
        timestamp = _timestamp(now)
        if ready_ids is not None and len(ready_ids) > (
            MAX_PENDING_NOTIFICATIONS + MAX_PENDING_PUSH_NOTIFICATIONS
        ):
            raise ValueError("Too many delivery candidates")
        with self._transaction() as connection:
            in_flight = connection.execute(
                "SELECT COUNT(*) FROM notification_outbox WHERE in_flight = 1 "
                "AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL"
            ).fetchone()[0]
            if in_flight >= MAX_IN_FLIGHT_NOTIFICATIONS:
                return None
            candidates = connection.execute(
                """SELECT o.* FROM notification_outbox AS o JOIN telegram_route AS r
                ON r.singleton = 1 AND o.destination = r.destination
                WHERE o.delivered_at IS NULL AND o.failed_at IS NULL AND o.blocked_by IS NULL
                AND o.in_flight = 0 AND NOT EXISTS (
                    SELECT 1 FROM notification_outbox AS earlier
                    WHERE earlier.source_alias = o.source_alias
                    AND earlier.destination = o.destination AND earlier.sequence < o.sequence
                    AND earlier.delivered_at IS NULL AND earlier.failed_at IS NULL
                    AND earlier.blocked_by IS NULL)
                ORDER BY o.sequence"""
            ).fetchall()
            row = next(
                (
                    row
                    for row in candidates
                    if (
                        row["notification_id"] in ready_ids
                        if ready_ids is not None
                        else row["next_attempt_at"] is None or row["next_attempt_at"] <= timestamp
                    )
                ),
                None,
            )
            if row is None:
                return None
            connection.execute(
                """UPDATE notification_outbox SET in_flight = 1, attempts = attempts + 1,
                attempted_at = ?, next_attempt_at = NULL WHERE notification_id = ?""",
                (timestamp, row["notification_id"]),
            )
            updated = connection.execute(
                "SELECT * FROM notification_outbox WHERE notification_id = ?",
                (row["notification_id"],),
            ).fetchone()
            assert updated is not None
            return _outbox_record(updated)

    def retry_delivery(
        self,
        notification_id: str,
        next_at: datetime,
        error: str,
        *,
        delay_seconds: float | None = None,
    ) -> None:
        """Persist an expected transient failure without acknowledging its send."""
        timestamp = _timestamp(next_at)
        _safe_error(error)
        with self._transaction() as connection:
            if delay_seconds is None:
                row = connection.execute(
                    "SELECT attempted_at FROM notification_outbox WHERE notification_id = ?",
                    (notification_id,),
                ).fetchone()
                attempted = None if row is None else _optional_datetime(row[0])
                delay_seconds = (
                    0 if attempted is None else max(0, (next_at - attempted).total_seconds())
                )
            delay = _retry_delay(delay_seconds)
            changed = connection.execute(
                """UPDATE notification_outbox SET in_flight = 0,
                next_attempt_at = ?, last_error = ?, retry_delay_seconds = ? WHERE notification_id = ?
                AND in_flight = 1 AND delivered_at IS NULL
                AND failed_at IS NULL AND blocked_by IS NULL""",
                (timestamp, error, delay, notification_id),
            )
            if changed.rowcount != 1:
                raise StorageError("An in-flight notification is required for retry")

    @staticmethod
    def _fail(connection: sqlite3.Connection, row: sqlite3.Row, timestamp: str, error: str) -> None:
        changed = connection.execute(
            """UPDATE notification_outbox SET in_flight = 0, failed_at = ?,
            next_attempt_at = NULL, last_error = ? WHERE notification_id = ?
            AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL""",
            (timestamp, error, row["notification_id"]),
        )
        if changed.rowcount == 0:
            return
        if row["kind"] == EventKind.DOWN.value:
            connection.execute(
                "UPDATE sites SET opening_failed = 1 WHERE opening_id = ?",
                (row["notification_id"],),
            )
            connection.execute(
                """UPDATE notification_outbox SET blocked_by = ?, in_flight = 0,
                next_attempt_at = NULL, last_error = 'opening_failed'
                WHERE source_alias = ? AND destination = ? AND incident_id = ?
                AND kind IN ('RECOVERY', 'REMINDER') AND delivered_at IS NULL
                AND failed_at IS NULL AND in_flight = 0""",
                (
                    row["notification_id"],
                    row["source_alias"],
                    row["destination"],
                    row["incident_id"],
                ),
            )
        elif row["kind"] == EventKind.REMINDER.value:
            connection.execute(
                """UPDATE sites SET reminder_failed = 1
                WHERE alias = ? AND delivery_destination = ?
                AND json_extract(state, '$.incident_id') = ?""",
                (row["source_alias"], row["destination"], row["incident_id"]),
            )
        elif row["kind"] == EventKind.FIRING.value:
            connection.execute(
                "UPDATE pushed_incidents SET opening_failed = 1 WHERE opening_id = ?",
                (row["notification_id"],),
            )
            connection.execute(
                """UPDATE notification_outbox SET blocked_by = ?, in_flight = 0,
                next_attempt_at = NULL, last_error = 'opening_failed'
                WHERE source_alias = ? AND destination = ? AND incident_id = ?
                AND kind = 'RESOLVED' AND delivered_at IS NULL
                AND failed_at IS NULL AND in_flight = 0""",
                (
                    row["notification_id"],
                    row["source_alias"],
                    row["destination"],
                    row["incident_id"],
                ),
            )

    def fail_delivery(self, notification_id: str, failed_at: datetime, error: str) -> None:
        """Retain a visible terminal failure and block its dependent recovery."""
        timestamp = _timestamp(failed_at)
        _safe_error(error)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM notification_outbox WHERE notification_id = ?", (notification_id,)
            ).fetchone()
            if row is None or row["delivered_at"] is not None or row["blocked_by"] is not None:
                raise StorageError("Pending notification does not exist")
            if row["failed_at"] is None:
                self._fail(connection, row, timestamp, error)
            self._prune_history(connection)

    def delivery_status(self) -> dict[str, int | str | None]:
        """Return bounded channel counters and a normalized last error."""
        with self._transaction() as connection:
            row = connection.execute(
                """SELECT
                COUNT(*) FILTER (WHERE delivered_at IS NULL AND failed_at IS NULL
                    AND blocked_by IS NULL) AS pending,
                COUNT(*) FILTER (WHERE delivered_at IS NULL AND failed_at IS NULL
                    AND blocked_by IS NULL AND attempts > 0) AS retrying,
                COUNT(*) FILTER (WHERE delivered_at IS NULL AND failed_at IS NOT NULL) AS failed,
                COUNT(*) FILTER (WHERE delivered_at IS NULL AND blocked_by IS NOT NULL) AS blocked,
                COUNT(*) FILTER (WHERE delivered_at IS NOT NULL) AS accepted,
                COUNT(*) FILTER (WHERE in_flight = 1) AS in_flight
                FROM notification_outbox WHERE destination LIKE 'telegram:%'"""
            ).fetchone()
            assert row is not None
            result: dict[str, int | str | None] = dict(row)
            last = connection.execute(
                """SELECT last_error FROM notification_outbox WHERE last_error IS NOT NULL
                AND destination LIKE 'telegram:%'
                ORDER BY COALESCE(failed_at, attempted_at, next_attempt_at) DESC,
                sequence DESC LIMIT 1"""
            ).fetchone()
            result["last_error"] = None if last is None else str(last[0])
            channel = connection.execute(
                "SELECT last_error, pause_until FROM telegram_route WHERE singleton = 1"
            ).fetchone()
            if channel is not None and channel[0] is not None:
                result["last_error"] = str(channel[0])
            result["paused"] = channel is not None and channel[1] is not None
            if result["paused"] and result["last_error"] not in {
                "forbidden",
                "bad_request",
                "unauthorized",
                "conflict",
                "webhook",
                "retries_exhausted",
                "destination_changed",
                "opening_failed",
            }:
                result["last_error"] = "rate_limit"
            return result

    def set_telegram_error(self, error: str | None) -> None:
        """Expose a normalized channel outage even when its outbox is empty."""
        if error is not None:
            _safe_error(error)
        with self._transaction() as connection:
            connection.execute(
                "UPDATE telegram_route SET last_error = ? WHERE singleton = 1", (error,)
            )

    def get_poll_cursor(self, bot_id: int) -> int:
        return self.get_poll_checkpoint(bot_id)[0]

    def get_poll_checkpoint(self, bot_id: int) -> tuple[int, datetime | None]:
        """Return offset and last accepted update time for bounded cursor lifetime."""
        _bot_id(bot_id)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT next_offset, received_at FROM telegram_updates WHERE bot_id = ?", (bot_id,)
            ).fetchone()
            if row is None:
                return 0, None
            try:
                return int(row[0]), _datetime(row[1])
            except (TypeError, ValueError) as exc:
                raise StorageError("Stored polling checkpoint is invalid") from exc

    def reset_poll_cursor(self, bot_id: int) -> None:
        """Forget an expired checkpoint without changing Telegram's remote queue."""
        _bot_id(bot_id)
        with self._transaction() as connection:
            connection.execute("DELETE FROM telegram_updates WHERE bot_id = ?", (bot_id,))

    def get_send_pause(self) -> tuple[datetime, float] | None:
        """Return a persisted shared Retry-After pause for conservative restore."""
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT pause_until, pause_delay_seconds FROM telegram_route WHERE singleton = 1"
            ).fetchone()
            if row is None or row[0] is None:
                return None
            try:
                return _datetime(row[0]), _retry_delay(row[1])
            except (TypeError, ValueError) as exc:
                raise StorageError("Stored send pause is invalid") from exc

    def set_send_pause(self, until: datetime, delay_seconds: float) -> None:
        timestamp = _timestamp(until)
        delay = _retry_delay(delay_seconds)
        with self._transaction() as connection:
            connection.execute(
                """UPDATE telegram_route SET pause_until = ?, pause_delay_seconds = ?
                WHERE singleton = 1""",
                (timestamp, delay),
            )

    def clear_send_pause(self) -> None:
        with self._transaction() as connection:
            connection.execute(
                "UPDATE telegram_route SET pause_until = NULL, pause_delay_seconds = NULL "
                "WHERE singleton = 1"
            )

    def set_poll_cursor(
        self, bot_id: int, next_offset: int, *, received_at: datetime | None = None
    ) -> None:
        """Advance the one configured bot's durable update cursor after handling."""
        _bot_id(bot_id)
        if type(next_offset) is not int or not 0 <= next_offset < 2**63:
            raise ValueError("Telegram update offset must be a nonnegative integer")
        timestamp = _timestamp(datetime.now(UTC) if received_at is None else received_at)
        with self._transaction() as connection:
            connection.execute("DELETE FROM telegram_updates WHERE bot_id != ?", (bot_id,))
            connection.execute(
                """INSERT INTO telegram_updates (bot_id, next_offset, received_at) VALUES (?, ?, ?)
                ON CONFLICT(bot_id) DO UPDATE SET
                received_at = CASE WHEN excluded.next_offset > next_offset
                    THEN excluded.received_at ELSE received_at END,
                next_offset = MAX(next_offset, excluded.next_offset)""",
                (bot_id, next_offset, timestamp),
            )

    @staticmethod
    def _prune_history(connection: sqlite3.Connection) -> None:
        connection.execute(
            """DELETE FROM notification_outbox
            WHERE (delivered_at IS NOT NULL OR failed_at IS NOT NULL OR blocked_by IS NOT NULL)
            AND sequence NOT IN (SELECT sequence FROM notification_outbox
                WHERE delivered_at IS NOT NULL OR failed_at IS NOT NULL OR blocked_by IS NOT NULL
                ORDER BY sequence DESC LIMIT ?)""",
            (HISTORY_LIMIT,),
        )

    def mark_in_flight(self, notification_id: str) -> None:
        """Protect the next ordered record while a delivery edge owns its send."""
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM notification_outbox WHERE notification_id = ?",
                (notification_id,),
            ).fetchone()
            if (
                row is None
                or row["delivered_at"] is not None
                or row["failed_at"] is not None
                or row["blocked_by"] is not None
            ):
                raise StorageError("Pending notification does not exist")
            in_flight = connection.execute(
                "SELECT COUNT(*) FROM notification_outbox "
                "WHERE delivered_at IS NULL AND in_flight = 1 "
                "AND failed_at IS NULL AND blocked_by IS NULL"
            ).fetchone()[0]
            if not row["in_flight"] and in_flight >= MAX_IN_FLIGHT_NOTIFICATIONS:
                raise StorageError("In-flight notification capacity is exhausted")
            earlier = connection.execute(
                """SELECT 1 FROM notification_outbox
                WHERE source_alias = ? AND destination = ?
                AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL
                AND sequence < ? LIMIT 1""",
                (row["source_alias"], row["destination"], row["sequence"]),
            ).fetchone()
            if earlier is not None:
                raise StorageError("An earlier notification must be delivered first")
            connection.execute(
                "UPDATE notification_outbox SET in_flight = 1 WHERE notification_id = ?",
                (notification_id,),
            )

    def release_in_flight(self, notification_id: str) -> None:
        """Keep a failed send pending for a later attempt."""
        with self._transaction() as connection:
            connection.execute(
                """UPDATE notification_outbox SET in_flight = 0
                WHERE notification_id = ? AND delivered_at IS NULL
                AND failed_at IS NULL AND blocked_by IS NULL""",
                (notification_id,),
            )

    def acknowledge(self, notification_id: str, delivered_at: datetime) -> None:
        """Record a provider-accepted send; callers never acknowledge failures."""
        timestamp = _timestamp(delivered_at)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM notification_outbox WHERE notification_id = ?",
                (notification_id,),
            ).fetchone()
            if row is None:
                raise StorageError("Notification does not exist")
            if row["delivered_at"] is not None:
                return
            if row["failed_at"] is not None or row["blocked_by"] is not None:
                raise StorageError("A terminal notification cannot be acknowledged")
            earlier = connection.execute(
                """SELECT 1 FROM notification_outbox WHERE source_alias = ?
                AND destination = ? AND delivered_at IS NULL
                AND failed_at IS NULL AND blocked_by IS NULL AND sequence < ? LIMIT 1""",
                (row["source_alias"], row["destination"], row["sequence"]),
            ).fetchone()
            if earlier is not None:
                raise StorageError("An earlier notification must be delivered first")
            connection.execute(
                """UPDATE notification_outbox SET delivered_at = ?, in_flight = 0
                WHERE notification_id = ?""",
                (timestamp, notification_id),
            )
            event = _outbox_record(row).event
            current = connection.execute(
                "SELECT * FROM sites WHERE alias = ?", (row["source_alias"],)
            ).fetchone()
            if (
                current is not None
                and _site_record(current).state.incident_id == event.incident_id
                and event.kind in (EventKind.DOWN, EventKind.REMINDER)
            ):
                connection.execute(
                    """INSERT INTO delivery_progress
                    (source_alias, destination, incident_id, last_delivered_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(source_alias, destination) DO UPDATE SET
                    incident_id = excluded.incident_id,
                    last_delivered_at = excluded.last_delivered_at""",
                    (row["source_alias"], row["destination"], event.incident_id, timestamp),
                )
            self._prune_history(connection)

    def reminder_progress(self) -> tuple[ReminderRecord, ...]:
        with self._transaction() as connection:
            return tuple(
                ReminderRecord(row[0], row[1], row[2], _datetime(row[3]))
                for row in connection.execute(
                    """SELECT p.source_alias, p.destination, p.incident_id, p.last_delivered_at
                    FROM delivery_progress AS p JOIN sites AS s ON p.source_alias = s.alias
                    WHERE p.incident_id = json_extract(s.state, '$.incident_id')
                    AND p.destination = s.delivery_destination
                    ORDER BY p.source_alias"""
                ).fetchall()
            )

    def queue_reminder(
        self, site: SiteConfig, now: datetime, fresh: bool, *, due: bool | None = None
    ) -> bool:
        """Queue at most one reminder after this route's accepted DOWN/reminder."""
        with self._transaction() as connection:
            record = self._read_site(connection, site.alias)
            if record.fingerprint != site.fingerprint or not site.enabled:
                raise StorageError("Source configuration was not reconciled")
            current = connection.execute(
                "SELECT delivery_destination, opening_failed, reminder_failed FROM sites WHERE alias = ?",
                (site.alias,),
            ).fetchone()
            assert current is not None
            destination = current[0] or "operator"
            if current[1] or current[2]:
                return False
            route = connection.execute(
                "SELECT destination FROM telegram_route WHERE singleton = 1"
            ).fetchone()
            if route is not None and route[0] is not None and route[0] != destination:
                return False
            if destination.startswith("telegram:") and (route is None or route[0] is None):
                return False
            pending = connection.execute(
                """SELECT 1 FROM notification_outbox WHERE source_alias = ?
                AND destination = ? AND kind = 'REMINDER'
                AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL LIMIT 1""",
                (site.alias, destination),
            ).fetchone()
            if pending is not None:
                return False
            last = connection.execute(
                """SELECT last_delivered_at FROM delivery_progress WHERE source_alias = ?
                AND destination = ? AND incident_id = ?""",
                (site.alias, destination, record.state.incident_id),
            ).fetchone()
            if due is None:
                event = due_reminder(
                    record.state,
                    now,
                    None if last is None else _datetime(last[0]),
                    site.reminder_interval,
                    fresh,
                )
            elif due and fresh and last is not None and record.state.confirmed is SiteState.DOWN:
                assert record.state.incident_id is not None
                assert record.state.incident_started_at is not None
                event = Event(
                    EventKind.REMINDER,
                    record.state.incident_id,
                    now,
                    record.state.incident_started_at,
                )
            else:
                event = None
            if event is None:
                return False
            self._queue_event(connection, site.alias, event)
            return True

    @staticmethod
    def _active_email_destinations(connection: sqlite3.Connection) -> tuple[str, ...]:
        return tuple(
            row[0]
            for row in connection.execute(
                "SELECT destination FROM email_routes WHERE enabled = 1 ORDER BY destination"
            ).fetchall()
        )

    @staticmethod
    def _prune_email_history(connection: sqlite3.Connection) -> None:
        connection.execute(
            """DELETE FROM email_outbox
            WHERE (delivered_at IS NOT NULL OR failed_at IS NOT NULL OR blocked_by IS NOT NULL)
            AND sequence NOT IN (SELECT sequence FROM email_outbox
                WHERE delivered_at IS NOT NULL OR failed_at IS NOT NULL OR blocked_by IS NOT NULL
                ORDER BY sequence DESC LIMIT ?)""",
            (HISTORY_LIMIT,),
        )

    @staticmethod
    def _insert_email(
        connection: sqlite3.Connection,
        alias: str,
        destination: str,
        event: Event,
        *,
        blocked_by: str | None = None,
    ) -> str:
        configured = tuple(
            row[0] for row in connection.execute("SELECT destination FROM email_routes").fetchall()
        )
        unavailable = bool(configured) and destination not in configured
        notification_id = str(uuid4())
        connection.execute(
            """INSERT INTO email_outbox
            (notification_id, source_alias, destination, incident_id, kind, event,
            blocked_by, failed_at, last_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                notification_id,
                alias,
                destination,
                event.incident_id,
                event.kind.value,
                _encode_event(event),
                blocked_by,
                _timestamp(event.occurred_at) if unavailable and blocked_by is None else None,
                "opening_failed"
                if blocked_by is not None
                else "destination_changed"
                if unavailable
                else None,
            ),
        )
        SQLiteStore._prune_email_history(connection)
        return notification_id

    @staticmethod
    def _queue_email_site_event(connection: sqlite3.Connection, alias: str, event: Event) -> None:
        if event.kind is EventKind.DOWN:
            destinations = SQLiteStore._active_email_destinations(connection)
            connection.execute("DELETE FROM email_progress WHERE source_alias = ?", (alias,))
            connection.execute("DELETE FROM email_site_routes WHERE source_alias = ?", (alias,))
            routes = [(destination, None) for destination in destinations]
        else:
            routes = [
                (row["destination"], row["opening_id"] if row["opening_failed"] else None)
                for row in connection.execute(
                    "SELECT * FROM email_site_routes WHERE source_alias = ? AND incident_id = ?",
                    (alias, event.incident_id),
                ).fetchall()
            ]
        configured = tuple(
            row[0] for row in connection.execute("SELECT destination FROM email_routes").fetchall()
        )
        for destination, blocked_by in routes:
            if event.kind is EventKind.DOWN:
                connection.execute(
                    """DELETE FROM email_outbox WHERE source_alias = ? AND destination = ?
                    AND incident_id != ? AND kind IN ('DOWN', 'RECOVERY', 'REMINDER')
                    AND delivered_at IS NULL AND in_flight = 0 AND attempts = 0
                    AND failed_at IS NULL AND blocked_by IS NULL""",
                    (alias, destination, event.incident_id),
                )
            else:
                connection.execute(
                    """DELETE FROM email_outbox WHERE source_alias = ? AND destination = ?
                    AND kind = 'REMINDER' AND delivered_at IS NULL AND in_flight = 0
                    AND attempts = 0 AND failed_at IS NULL AND blocked_by IS NULL""",
                    (alias, destination),
                )
            pending = connection.execute(
                "SELECT COUNT(*) FROM email_outbox WHERE destination = ? "
                "AND kind IN ('DOWN', 'RECOVERY', 'REMINDER') AND delivered_at IS NULL "
                "AND failed_at IS NULL AND blocked_by IS NULL",
                (destination,),
            ).fetchone()[0]
            if (
                blocked_by is None
                and (not configured or destination in configured)
                and pending >= MAX_PENDING_NOTIFICATIONS
            ):
                raise StorageError("Email notification outbox capacity is exhausted")
            notification_id = SQLiteStore._insert_email(
                connection, alias, destination, event, blocked_by=blocked_by
            )
            if event.kind is EventKind.DOWN:
                connection.execute(
                    """INSERT INTO email_site_routes
                    (source_alias, destination, incident_id, opening_id) VALUES (?, ?, ?, ?)""",
                    (alias, destination, event.incident_id, notification_id),
                )

    @staticmethod
    def _email_event_capacity(
        connection: sqlite3.Connection,
        alias: str,
        limits: IngressSettings,
        destinations: Sequence[str],
        *,
        active_delta: int,
        delivery_delta: int,
    ) -> None:
        for destination in destinations:
            for source in (None, alias):
                condition = "" if source is None else " AND source_alias = ?"
                values = (destination,) if source is None else (destination, source)
                pending = connection.execute(
                    "SELECT COUNT(*) FROM email_outbox WHERE destination = ? "
                    "AND kind IN ('NOTIFICATION', 'FIRING', 'RESOLVED') "
                    "AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL"
                    + condition,
                    values,
                ).fetchone()[0]
                active = connection.execute(
                    "SELECT COUNT(*) FROM email_push_routes AS r JOIN pushed_incidents AS p "
                    "ON p.source_alias = r.source_alias AND p.incident_key = r.incident_key "
                    "WHERE r.destination = ? AND p.resolved_at IS NULL"
                    + ("" if source is None else " AND r.source_alias = ?"),
                    values,
                ).fetchone()[0]
                maximum = limits.max_pending if source is None else limits.max_pending_per_source
                if pending + delivery_delta + active + active_delta > maximum:
                    raise EventRejected("capacity")

    @staticmethod
    def _queue_email_push_event(
        connection: sqlite3.Connection, alias: str, event: Event, opening: bool
    ) -> None:
        if event.kind is EventKind.RESOLVED:
            routes = [
                (row["destination"], row["opening_id"] if row["opening_failed"] else None)
                for row in connection.execute(
                    "SELECT * FROM email_push_routes WHERE source_alias = ? AND incident_key = ?",
                    (alias, event.incident_id),
                ).fetchall()
            ]
        else:
            routes = [
                (destination, None)
                for destination in SQLiteStore._active_email_destinations(connection)
            ]
        for destination, blocked_by in routes:
            notification_id = SQLiteStore._insert_email(
                connection, alias, destination, event, blocked_by=blocked_by
            )
            if opening:
                connection.execute(
                    """INSERT INTO email_push_routes
                    (source_alias, incident_key, destination, opening_id) VALUES (?, ?, ?, ?)""",
                    (alias, event.incident_id, destination, notification_id),
                )

    def configure_email(self, destinations: Sequence[str], now: datetime) -> int:
        """Restore interrupted attempts and bind only operator-owned opaque routes.

        An empty route set pauses saved deliveries. A changed nonempty set fails
        removed destinations instead of rerouting their accepted incident history.
        """
        keys = tuple(destinations)
        if (
            len(keys) > 5
            or len(set(keys)) != len(keys)
            or any(re.fullmatch(r"email:[0-9a-f]{64}", key) is None for key in keys)
        ):
            raise ValueError("Email requires at most five unique opaque destinations")
        timestamp = _timestamp(now)
        with self._transaction() as connection:
            interrupted = connection.execute(
                "SELECT COUNT(*) FROM email_outbox WHERE in_flight = 1 "
                "AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL"
            ).fetchone()[0]
            connection.execute(
                """UPDATE email_outbox SET in_flight = 0, attempts = MAX(attempts, 1),
                next_attempt_at = ?, last_error = 'interrupted' WHERE in_flight = 1
                AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL""",
                (timestamp,),
            )
            if keys:
                connection.execute("UPDATE email_channel SET suspended = 0 WHERE singleton = 1")
                rows = connection.execute(
                    "SELECT * FROM email_outbox WHERE delivered_at IS NULL "
                    "AND failed_at IS NULL AND blocked_by IS NULL ORDER BY sequence"
                ).fetchall()
                for row in rows:
                    if row["destination"] not in keys:
                        self._fail_email(connection, row, timestamp, "destination_changed")
                connection.execute("DELETE FROM email_routes")
                connection.executemany(
                    "INSERT INTO email_routes VALUES (?, 1)", ((key,) for key in keys)
                )
                for destination in keys:
                    connection.execute(
                        """DELETE FROM email_outbox AS old
                        WHERE old.destination = ? AND old.delivered_at IS NULL
                        AND old.kind IN ('DOWN', 'RECOVERY', 'REMINDER')
                        AND old.failed_at IS NULL AND old.blocked_by IS NULL
                        AND old.in_flight = 0 AND old.attempts = 0
                        AND EXISTS (SELECT 1 FROM email_outbox AS latest
                            WHERE latest.source_alias = old.source_alias
                            AND latest.destination = old.destination AND latest.kind = 'DOWN'
                            AND latest.sequence = (SELECT MAX(sequence) FROM email_outbox
                                WHERE source_alias = old.source_alias
                                AND destination = old.destination AND kind = 'DOWN')
                            AND latest.incident_id != old.incident_id)""",
                        (destination,),
                    )
            else:
                connection.execute("UPDATE email_routes SET enabled = 0")
            self._prune_email_history(connection)
            return int(interrupted)

    def pending_email_notifications(self) -> tuple[OutboxRecord, ...]:
        with self._transaction() as connection:
            return tuple(
                _outbox_record(row)
                for row in connection.execute(
                    "SELECT * FROM email_outbox WHERE delivered_at IS NULL "
                    "AND failed_at IS NULL AND blocked_by IS NULL ORDER BY sequence"
                ).fetchall()
            )

    def email_pending_schedule(self) -> tuple[EmailDeliverySchedule, ...]:
        """Read bounded retry metadata without loading or decoding event payloads."""
        maximum = 5 * (MAX_PENDING_NOTIFICATIONS + MAX_PENDING_PUSH_NOTIFICATIONS)
        with self._transaction() as connection:
            rows = connection.execute(
                "SELECT notification_id, destination, in_flight, attempts, retry_delay_seconds "
                "FROM email_outbox WHERE delivered_at IS NULL "
                "AND failed_at IS NULL AND blocked_by IS NULL ORDER BY sequence LIMIT ?",
                (maximum + 1,),
            ).fetchall()
            if len(rows) > maximum:
                raise StorageError("Email notification outbox exceeds its capacity")
            return tuple(
                EmailDeliverySchedule(row[0], row[1], bool(row[2]), row[3], row[4]) for row in rows
            )

    def email_pending_count(self) -> int:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT COUNT(*) FROM email_outbox WHERE delivered_at IS NULL "
                "AND failed_at IS NULL AND blocked_by IS NULL"
            ).fetchone()
            assert row is not None
            return int(row[0])

    def reserve_due_email_delivery(
        self, now: datetime, *, ready_ids: Sequence[str] | None = None
    ) -> OutboxRecord | None:
        """Persist one per-recipient attempt without blocking other recipients."""
        timestamp = _timestamp(now)
        if ready_ids is not None and len(ready_ids) > 5 * (
            MAX_PENDING_NOTIFICATIONS + MAX_PENDING_PUSH_NOTIFICATIONS
        ):
            raise ValueError("Too many email delivery candidates")
        with self._transaction() as connection:
            in_flight = connection.execute(
                "SELECT COUNT(*) FROM email_outbox WHERE in_flight = 1 "
                "AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL"
            ).fetchone()[0]
            if in_flight >= MAX_IN_FLIGHT_EMAIL:
                return None
            candidates = connection.execute(
                """SELECT o.notification_id, o.next_attempt_at FROM email_outbox AS o
                JOIN email_routes AS r
                ON r.enabled = 1 AND o.destination = r.destination
                JOIN email_channel AS c ON c.singleton = 1 AND c.suspended = 0
                WHERE o.delivered_at IS NULL AND o.failed_at IS NULL AND o.blocked_by IS NULL
                AND o.in_flight = 0 AND NOT EXISTS (
                    SELECT 1 FROM email_outbox AS earlier
                    WHERE earlier.source_alias = o.source_alias
                    AND earlier.destination = o.destination AND earlier.sequence < o.sequence
                    AND earlier.delivered_at IS NULL AND earlier.failed_at IS NULL
                    AND earlier.blocked_by IS NULL) ORDER BY o.sequence"""
            ).fetchall()
            row = next(
                (
                    row
                    for row in candidates
                    if (
                        row["notification_id"] in ready_ids
                        if ready_ids is not None
                        else row["next_attempt_at"] is None or row["next_attempt_at"] <= timestamp
                    )
                ),
                None,
            )
            if row is None:
                return None
            connection.execute(
                "UPDATE email_outbox SET in_flight = 1, attempts = attempts + 1, "
                "attempted_at = ?, next_attempt_at = NULL WHERE notification_id = ?",
                (timestamp, row["notification_id"]),
            )
            updated = connection.execute(
                "SELECT * FROM email_outbox WHERE notification_id = ?", (row["notification_id"],)
            ).fetchone()
            assert updated is not None
            return _outbox_record(updated)

    def retry_email_delivery(
        self,
        notification_id: str,
        next_at: datetime,
        error: str,
        *,
        delay_seconds: float | None = None,
    ) -> None:
        """Release a failed attempt while preserving its independent retry clock."""
        timestamp = _timestamp(next_at)
        _safe_error(error)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT attempted_at FROM email_outbox WHERE notification_id = ?",
                (notification_id,),
            ).fetchone()
            attempted = None if row is None else _optional_datetime(row[0])
            delay = _retry_delay(
                (0 if attempted is None else max(0, (next_at - attempted).total_seconds()))
                if delay_seconds is None
                else delay_seconds
            )
            changed = connection.execute(
                """UPDATE email_outbox SET in_flight = 0, next_attempt_at = ?,
                last_error = ?, retry_delay_seconds = ? WHERE notification_id = ?
                AND in_flight = 1 AND delivered_at IS NULL
                AND failed_at IS NULL AND blocked_by IS NULL""",
                (timestamp, error, delay, notification_id),
            )
            if changed.rowcount != 1:
                raise StorageError("An in-flight email notification is required for retry")

    @staticmethod
    def _fail_email(
        connection: sqlite3.Connection, row: sqlite3.Row, timestamp: str, error: str
    ) -> None:
        changed = connection.execute(
            """UPDATE email_outbox SET in_flight = 0, failed_at = ?,
            next_attempt_at = NULL, last_error = ? WHERE notification_id = ?
            AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL""",
            (timestamp, error, row["notification_id"]),
        )
        if changed.rowcount == 0:
            return
        if row["kind"] in (EventKind.DOWN.value, EventKind.FIRING.value):
            table = (
                "email_site_routes" if row["kind"] == EventKind.DOWN.value else "email_push_routes"
            )
            connection.execute(
                f"UPDATE {table} SET opening_failed = 1 WHERE opening_id = ?",
                (row["notification_id"],),
            )
            kinds = (
                "('RECOVERY', 'REMINDER')"
                if row["kind"] == EventKind.DOWN.value
                else "('RESOLVED')"
            )
            connection.execute(
                "UPDATE email_outbox SET blocked_by = ?, in_flight = 0, "
                "next_attempt_at = NULL, last_error = 'opening_failed' "
                "WHERE source_alias = ? AND destination = ? AND incident_id = ? "
                f"AND kind IN {kinds} AND delivered_at IS NULL "
                "AND failed_at IS NULL AND in_flight = 0",
                (
                    row["notification_id"],
                    row["source_alias"],
                    row["destination"],
                    row["incident_id"],
                ),
            )
        elif row["kind"] == EventKind.REMINDER.value:
            connection.execute(
                "UPDATE email_site_routes SET reminder_failed = 1 WHERE source_alias = ? "
                "AND destination = ? AND incident_id = ?",
                (row["source_alias"], row["destination"], row["incident_id"]),
            )

    def fail_email_delivery(self, notification_id: str, failed_at: datetime, error: str) -> None:
        """Persist a terminal destination outcome and dependent lifecycle blockers."""
        timestamp = _timestamp(failed_at)
        _safe_error(error)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM email_outbox WHERE notification_id = ?", (notification_id,)
            ).fetchone()
            if row is None or row["delivered_at"] is not None or row["blocked_by"] is not None:
                raise StorageError("Pending email notification does not exist")
            if row["failed_at"] is None:
                self._fail_email(connection, row, timestamp, error)
            self._prune_email_history(connection)

    def acknowledge_email(self, notification_id: str, delivered_at: datetime) -> None:
        """Complete only this accepted recipient; no other recipient is retried."""
        timestamp = _timestamp(delivered_at)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM email_outbox WHERE notification_id = ?", (notification_id,)
            ).fetchone()
            if row is None:
                raise StorageError("Email notification does not exist")
            if row["delivered_at"] is not None:
                return
            if row["failed_at"] is not None or row["blocked_by"] is not None:
                raise StorageError("A terminal email notification cannot be acknowledged")
            earlier = connection.execute(
                "SELECT 1 FROM email_outbox WHERE source_alias = ? AND destination = ? "
                "AND delivered_at IS NULL AND failed_at IS NULL AND blocked_by IS NULL "
                "AND sequence < ? LIMIT 1",
                (row["source_alias"], row["destination"], row["sequence"]),
            ).fetchone()
            if earlier is not None:
                raise StorageError("An earlier email notification must be delivered first")
            connection.execute(
                "UPDATE email_outbox SET delivered_at = ?, in_flight = 0 WHERE notification_id = ?",
                (timestamp, notification_id),
            )
            event = _outbox_record(row).event
            current = connection.execute(
                "SELECT 1 FROM email_site_routes WHERE source_alias = ? "
                "AND destination = ? AND incident_id = ?",
                (row["source_alias"], row["destination"], event.incident_id),
            ).fetchone()
            if current is not None and event.kind in (EventKind.DOWN, EventKind.REMINDER):
                connection.execute(
                    """INSERT INTO email_progress VALUES (?, ?, ?, ?)
                    ON CONFLICT(source_alias, destination) DO UPDATE SET
                    incident_id = excluded.incident_id, last_delivered_at = excluded.last_delivered_at""",
                    (row["source_alias"], row["destination"], event.incident_id, timestamp),
                )
            self._prune_email_history(connection)

    def email_reminder_progress(self) -> tuple[ReminderRecord, ...]:
        with self._transaction() as connection:
            return tuple(
                ReminderRecord(row[0], row[1], row[2], _datetime(row[3]))
                for row in connection.execute(
                    """SELECT p.source_alias, p.destination, p.incident_id, p.last_delivered_at
                    FROM email_progress AS p JOIN email_site_routes AS r
                    ON r.source_alias = p.source_alias AND r.destination = p.destination
                    AND r.incident_id = p.incident_id JOIN sites AS s ON s.alias = p.source_alias
                    WHERE p.incident_id = json_extract(s.state, '$.incident_id')
                    ORDER BY p.source_alias, p.destination"""
                ).fetchall()
            )

    def queue_email_reminder(
        self,
        site: SiteConfig,
        destination: str,
        now: datetime,
        fresh: bool,
        *,
        due: bool | None = None,
    ) -> bool:
        """Queue one fresh reminder using this recipient's accepted delivery clock."""
        with self._transaction() as connection:
            record = self._read_site(connection, site.alias)
            if record.fingerprint != site.fingerprint or not site.enabled:
                raise StorageError("Source configuration was not reconciled")
            route = connection.execute(
                """SELECT r.* FROM email_site_routes AS r JOIN email_routes AS c
                ON c.destination = r.destination AND c.enabled = 1
                WHERE r.source_alias = ? AND r.destination = ? AND r.incident_id = ?""",
                (site.alias, destination, record.state.incident_id),
            ).fetchone()
            if route is None or route["opening_failed"] or route["reminder_failed"]:
                return False
            pending = connection.execute(
                "SELECT 1 FROM email_outbox WHERE source_alias = ? AND destination = ? "
                "AND kind = 'REMINDER' AND delivered_at IS NULL AND failed_at IS NULL "
                "AND blocked_by IS NULL LIMIT 1",
                (site.alias, destination),
            ).fetchone()
            if pending is not None:
                return False
            last = connection.execute(
                "SELECT last_delivered_at FROM email_progress WHERE source_alias = ? "
                "AND destination = ? AND incident_id = ?",
                (site.alias, destination, record.state.incident_id),
            ).fetchone()
            if due is None:
                event = due_reminder(
                    record.state,
                    now,
                    None if last is None else _datetime(last[0]),
                    site.reminder_interval,
                    fresh,
                )
            elif due and fresh and last is not None and record.state.confirmed is SiteState.DOWN:
                assert record.state.incident_id is not None
                assert record.state.incident_started_at is not None
                event = Event(
                    EventKind.REMINDER,
                    record.state.incident_id,
                    now,
                    record.state.incident_started_at,
                )
            else:
                event = None
            if event is None:
                return False
            pending_count = connection.execute(
                "SELECT COUNT(*) FROM email_outbox WHERE destination = ? "
                "AND kind IN ('DOWN', 'RECOVERY', 'REMINDER') AND delivered_at IS NULL "
                "AND failed_at IS NULL AND blocked_by IS NULL",
                (destination,),
            ).fetchone()[0]
            if pending_count >= MAX_PENDING_NOTIFICATIONS:
                return False
            self._insert_email(connection, site.alias, destination, event)
            return True

    def get_email_pause(self) -> tuple[datetime, float] | None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT pause_until, pause_delay_seconds FROM email_channel WHERE singleton = 1"
            ).fetchone()
            if row is None or row[0] is None:
                return None
            return _datetime(row[0]), _retry_delay(row[1])

    def set_email_pause(self, until: datetime, delay_seconds: float) -> None:
        timestamp = _timestamp(until)
        delay = _retry_delay(delay_seconds)
        with self._transaction() as connection:
            connection.execute(
                "UPDATE email_channel SET pause_until = ?, pause_delay_seconds = ? WHERE singleton = 1",
                (timestamp, delay),
            )

    def clear_email_pause(self) -> None:
        with self._transaction() as connection:
            connection.execute(
                "UPDATE email_channel SET pause_until = NULL, pause_delay_seconds = NULL "
                "WHERE singleton = 1"
            )

    def set_email_error(self, error: str | None) -> None:
        if error is not None:
            _safe_error(error)
        with self._transaction() as connection:
            connection.execute(
                "UPDATE email_channel SET last_error = ? WHERE singleton = 1", (error,)
            )

    def set_email_suspended(self, suspended: bool) -> None:
        """Pause a misconfigured provider without failing other destinations."""
        if type(suspended) is not bool:
            raise ValueError("Email suspension must be a boolean")
        with self._transaction() as connection:
            connection.execute(
                "UPDATE email_channel SET suspended = ? WHERE singleton = 1", (int(suspended),)
            )

    def email_delivery_status(self, destination: str | None = None) -> dict[str, int | str | None]:
        """Expose channel counters and categories without addresses or route keys."""
        with self._transaction() as connection:
            row = connection.execute(
                """SELECT COUNT(*) FILTER (WHERE delivered_at IS NULL AND failed_at IS NULL
                    AND blocked_by IS NULL) AS pending,
                COUNT(*) FILTER (WHERE delivered_at IS NULL AND failed_at IS NULL
                    AND blocked_by IS NULL AND attempts > 0) AS retrying,
                COUNT(*) FILTER (WHERE delivered_at IS NULL AND failed_at IS NOT NULL) AS failed,
                COUNT(*) FILTER (WHERE delivered_at IS NULL AND blocked_by IS NOT NULL) AS blocked,
                COUNT(*) FILTER (WHERE delivered_at IS NOT NULL) AS accepted,
                    COUNT(*) FILTER (WHERE in_flight = 1) AS in_flight FROM email_outbox"""
                + ("" if destination is None else " WHERE destination = ?"),
                () if destination is None else (destination,),
            ).fetchone()
            assert row is not None
            result: dict[str, int | str | None] = dict(row)
            last = connection.execute(
                "SELECT last_error FROM email_outbox WHERE last_error IS NOT NULL "
                + ("" if destination is None else "AND destination = ? ")
                + "ORDER BY COALESCE(failed_at, attempted_at, next_attempt_at) DESC, "
                "sequence DESC LIMIT 1",
                () if destination is None else (destination,),
            ).fetchone()
            result["last_error"] = None if last is None else str(last[0])
            channel = connection.execute(
                "SELECT pause_until, last_error, suspended FROM email_channel WHERE singleton = 1"
            ).fetchone()
            if channel is not None and channel[1] is not None:
                result["last_error"] = str(channel[1])
            result["paused"] = (
                not bool(self._active_email_destinations(connection))
                or channel is not None
                and (channel[0] is not None or bool(channel[2]))
            )
            return result
