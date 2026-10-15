"""Transactional monitoring snapshots and a bounded notification outbox."""

from __future__ import annotations

import json
import math
import re
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from sentinel_bot.config import SiteConfig
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

SCHEMA_VERSION = 3
DATABASE_NAME = "state.sqlite3"
HISTORY_LIMIT = 200
MAX_PENDING_NOTIFICATIONS = 80
MAX_IN_FLIGHT_NOTIFICATIONS = 20
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
    elif version not in (1, 2, SCHEMA_VERSION):
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
    ):
        connection.execute(query)
    if connection.execute("SELECT 1 FROM telegram_route WHERE singleton = 1").fetchone() is None:
        raise StorageError("Telegram route metadata is missing")


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
    return json.dumps(
        {
            "kind": event.kind.value,
            "incident_id": event.incident_id,
            "occurred_at": _timestamp(event.occurred_at),
            "started_at": _timestamp(event.started_at),
        },
        separators=(",", ":"),
    )


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
        connection.execute(
            """DELETE FROM notification_outbox
            WHERE source_alias = ? AND delivered_at IS NULL AND in_flight = 0
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
        route = connection.execute(
            "SELECT destination FROM telegram_route WHERE singleton = 1"
        ).fetchone()
        destination = "operator" if route is None or route[0] is None else str(route[0])
        current = connection.execute(
            "SELECT delivery_destination, opening_id, opening_failed FROM sites WHERE alias = ?",
            (alias,),
        ).fetchone()
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
            "SELECT COUNT(*) FROM notification_outbox WHERE delivered_at IS NULL "
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
                "SELECT COUNT(*) FROM notification_outbox WHERE delivered_at IS NULL "
                "AND failed_at IS NULL AND blocked_by IS NULL"
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
        if ready_ids is not None and len(ready_ids) > MAX_PENDING_NOTIFICATIONS:
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
