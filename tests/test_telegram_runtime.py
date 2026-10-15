import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import sentinel_bot.telegram_runtime as telegram
from sentinel_bot.config import Settings, SiteConfig, TelegramSettings
from sentinel_bot.models import CheckResult, ErrorKind, EventKind, Observation
from sentinel_bot.monitoring import MonitoringProgress
from sentinel_bot.storage import SQLiteStore, initialize_state
from sentinel_bot.telegram_api import BotIdentity, TelegramError, TelegramErrorKind

START = datetime(2026, 1, 1, tzinfo=UTC)
CONFIG = TelegramSettings("123456:synthetic_credential_abcdefghijklmnop", -100, frozenset({1}), 7)


def prepare(tmp_path: Path, *, with_site: bool = True) -> tuple[Settings, SQLiteStore]:
    sites = (
        (
            SiteConfig(
                "primary", "https://example.test/", failure_confirmation=1, recovery_confirmation=1
            ),
        )
        if with_site
        else ()
    )
    settings = Settings(tmp_path, mode="monitor" if sites else "idle", sites=sites, telegram=CONFIG)
    initialize_state(tmp_path, START)
    store = SQLiteStore(tmp_path)
    store.reconcile(sites)
    store.configure_telegram(CONFIG.destination, START)
    return settings, store


def failure(store: SQLiteStore, settings: Settings) -> None:
    store.apply_observation(
        settings.sites[0], Observation(START, CheckResult(False, ErrorKind.TIMEOUT))
    )


def recovery(store: SQLiteStore, settings: Settings) -> None:
    store.apply_observation(
        settings.sites[0],
        Observation(START + timedelta(seconds=1), CheckResult(True, http_status=200)),
    )


def update(
    identifier: int, *, user: int = 1, chat: int = -100, topic: int = 7
) -> dict[str, object]:
    return {
        "update_id": identifier,
        "message": {
            "chat": {"id": chat, "type": "supergroup"},
            "from": {"id": user, "is_bot": False},
            "message_thread_id": topic,
            "text": "/status@Sentinel_Test_Bot",
        },
    }


async def eventually(condition: Callable[[], bool]) -> None:
    async with asyncio.timeout(3):
        while not condition():
            await asyncio.sleep(0.001)


class FakeAPI:
    def __init__(self) -> None:
        self.updates: asyncio.Queue[tuple[dict[str, object], ...] | BaseException] = asyncio.Queue()
        self.offsets: list[int] = []
        self.sent: list[tuple[int, str, int | None]] = []
        self.accepted: list[tuple[int, str, int | None]] = []
        self.send_results: deque[int | BaseException] = deque()
        self.startup_results: deque[BotIdentity | BaseException] = deque()
        self.startups = 0
        self.closed = False
        self.poll_cancelled = False
        self.send_started: asyncio.Event | None = None
        self.send_block: asyncio.Event | None = None

    async def __aenter__(self) -> FakeAPI:
        return self

    async def __aexit__(self, *args: object) -> None:
        self.closed = True

    async def startup(self) -> BotIdentity:
        self.startups += 1
        result = (
            self.startup_results.popleft()
            if self.startup_results
            else BotIdentity(123456, "Sentinel_Test_Bot")
        )
        if isinstance(result, BaseException):
            raise result
        return result

    async def get_updates(self, offset: int) -> tuple[dict[str, object], ...]:
        self.offsets.append(offset)
        try:
            result = await self.updates.get()
        except asyncio.CancelledError:
            self.poll_cancelled = True
            raise
        if isinstance(result, BaseException):
            raise result
        return result

    async def send_message(self, chat_id: int, text: str, *, topic_id: int | None = None) -> int:
        item = (chat_id, text, topic_id)
        self.sent.append(item)
        if self.send_started is not None:
            self.send_started.set()
        if self.send_block is not None:
            await self.send_block.wait()
        result = self.send_results.popleft() if self.send_results else len(self.sent)
        if isinstance(result, BaseException):
            raise result
        self.accepted.append(item)
        return result


def fake_boundary(monkeypatch: pytest.MonkeyPatch, api: FakeAPI) -> None:
    monkeypatch.setattr(telegram, "TelegramAPI", lambda token: api)
    monkeypatch.setattr(telegram, "SEND_INTERVAL", 0.0)


def test_authorized_cached_reply_ignores_other_actors_and_preserves_cursor_on_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path)
    store.apply_observation(
        settings.sites[0], Observation(START, CheckResult(True, http_status=200))
    )

    async def reject_probe(*args: object, **kwargs: object) -> None:
        pytest.fail("Telegram commands must not start website checks")

    monkeypatch.setattr("sentinel_bot.http.HttpChecker.check", reject_probe)

    async def scenario() -> None:
        api = FakeAPI()
        fake_boundary(monkeypatch, api)
        api.updates.put_nowait(
            (update(10, chat=-200), update(11, user=2), update(12, topic=8), update(13))
        )
        stopped = asyncio.Event()
        progress = MonitoringProgress()
        task = asyncio.create_task(
            telegram.run_telegram(settings, stopped, progress, asyncio.Lock())
        )
        try:
            await eventually(
                lambda: (
                    len(api.accepted) == 1
                    and store.get_poll_cursor(123456) == 14
                    and len(api.offsets) >= 2
                )
            )
            chat_id, text, topic_id = api.accepted[0]
            assert (chat_id, topic_id) == (-100, 7)
            assert "primary: UP; stale; passed HTTP200" in text
            assert "example.test" not in text
            assert set(progress.telegram) == telegram.WORKER_NAMES
        finally:
            stopped.set()
            await asyncio.wait_for(task, 2)
        assert api.closed and api.poll_cancelled

        restarted = FakeAPI()
        fake_boundary(monkeypatch, restarted)
        stop_again = asyncio.Event()
        task = asyncio.create_task(
            telegram.run_telegram(settings, stop_again, MonitoringProgress(), asyncio.Lock())
        )
        try:
            await eventually(lambda: bool(restarted.offsets))
            assert restarted.offsets == [14]
            assert restarted.sent == []
        finally:
            stop_again.set()
            await asyncio.wait_for(task, 2)

    asyncio.run(scenario())


def test_unauthorized_and_malformed_updates_never_read_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path, with_site=False)

    def reject_snapshot(*args: object, **kwargs: object) -> None:
        pytest.fail("Unauthorized messages must not read operational status")

    monkeypatch.setattr("sentinel_bot.status.status_snapshot", reject_snapshot)

    async def scenario() -> None:
        api = FakeAPI()
        fake_boundary(monkeypatch, api)
        api.updates.put_nowait((update(1, user=2), {"update_id": 2, "message": []}))
        stopped = asyncio.Event()
        task = asyncio.create_task(
            telegram.run_telegram(settings, stopped, MonitoringProgress(), asyncio.Lock())
        )
        try:
            await eventually(lambda: store.get_poll_cursor(123456) == 3)
            assert api.sent == []
        finally:
            stopped.set()
            await asyncio.wait_for(task, 2)

    asyncio.run(scenario())


def test_transient_incident_retry_acknowledges_only_provider_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path)
    failure(store, settings)
    notification_id = store.pending_notifications()[0].id
    monkeypatch.setattr(telegram, "_retry_delay", lambda attempt, error: 0)

    async def scenario() -> None:
        api = FakeAPI()
        api.send_results.extend([TelegramError(TelegramErrorKind.TRANSIENT, status=503), 99])
        fake_boundary(monkeypatch, api)
        stopped = asyncio.Event()
        task = asyncio.create_task(
            telegram.run_telegram(settings, stopped, MonitoringProgress(), asyncio.Lock())
        )
        try:
            await eventually(lambda: store.delivery_status()["accepted"] == 1)
            assert len(api.sent) == 2
            assert api.sent[0] == api.sent[1]
            assert len(api.accepted) == 1
            assert notification_id not in {record.id for record in store.pending_notifications()}
            assert len(store.reminder_progress()) == 1
        finally:
            stopped.set()
            await asyncio.wait_for(task, 2)

    asyncio.run(scenario())


def test_permanent_opening_failure_blocks_recovery_without_sending_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path)
    failure(store, settings)
    recovery(store, settings)

    async def scenario() -> None:
        api = FakeAPI()
        api.send_results.append(TelegramError(TelegramErrorKind.PERMANENT, status=403))
        fake_boundary(monkeypatch, api)
        stopped = asyncio.Event()
        task = asyncio.create_task(
            telegram.run_telegram(settings, stopped, MonitoringProgress(), asyncio.Lock())
        )
        try:
            await eventually(lambda: store.delivery_status()["blocked"] == 1)
            assert store.delivery_status()["failed"] == 1
            assert store.delivery_status()["last_error"] == "forbidden"
            assert len(api.sent) == 1
            assert api.sent[0][1].startswith("DOWN: primary")
            assert api.accepted == []
            assert store.pending_count() == 0
        finally:
            stopped.set()
            await asyncio.wait_for(task, 2)

    asyncio.run(scenario())


def test_down_is_provider_accepted_before_matching_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path)
    failure(store, settings)
    recovery(store, settings)

    async def scenario() -> None:
        api = FakeAPI()
        fake_boundary(monkeypatch, api)
        stopped = asyncio.Event()
        task = asyncio.create_task(
            telegram.run_telegram(settings, stopped, MonitoringProgress(), asyncio.Lock())
        )
        try:
            await eventually(lambda: store.delivery_status()["accepted"] == 2)
            assert [item[1].split(":", 1)[0] for item in api.accepted] == ["DOWN", "RECOVERY"]
            assert store.reminder_progress() == ()
        finally:
            stopped.set()
            await asyncio.wait_for(task, 2)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "kind", [TelegramErrorKind.UNAUTHORIZED, TelegramErrorKind.CONFLICT, TelegramErrorKind.WEBHOOK]
)
def test_fatal_startup_errors_are_safe_configuration_failures_and_close_api(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: TelegramErrorKind
) -> None:
    settings, store = prepare(tmp_path)

    async def scenario() -> None:
        api = FakeAPI()
        api.startup_results.append(TelegramError(kind))
        fake_boundary(monkeypatch, api)
        with pytest.raises(telegram.TelegramConfigurationError, match=kind.value):
            await telegram.run_telegram(
                settings, asyncio.Event(), MonitoringProgress(), asyncio.Lock()
            )
        assert api.closed
        assert api.sent == []
        assert store.pending_count() == 0

    asyncio.run(scenario())


def test_expected_polling_outage_does_not_stop_incident_delivery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path)
    failure(store, settings)
    monkeypatch.setattr(telegram, "_retry_delay", lambda attempt, error: 0)

    async def scenario() -> None:
        api = FakeAPI()
        api.updates.put_nowait(TelegramError(TelegramErrorKind.TRANSIENT))
        api.updates.put_nowait((update(10),))
        fake_boundary(monkeypatch, api)
        stopped = asyncio.Event()
        task = asyncio.create_task(
            telegram.run_telegram(settings, stopped, MonitoringProgress(), asyncio.Lock())
        )
        try:
            await eventually(
                lambda: store.delivery_status()["accepted"] == 1 and len(api.accepted) == 2
            )
            assert any(item[1].startswith("DOWN:") for item in api.accepted)
            assert any(item[1].startswith("Sentinel status") for item in api.accepted)
            assert len(api.offsets) >= 2
            assert not task.done()
        finally:
            stopped.set()
            await asyncio.wait_for(task, 2)

    asyncio.run(scenario())


def test_unexpected_worker_error_cancels_siblings_and_closes_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, _ = prepare(tmp_path, with_site=False)

    async def scenario() -> None:
        api = FakeAPI()
        api.updates.put_nowait(RuntimeError("synthetic programming failure"))
        fake_boundary(monkeypatch, api)
        with pytest.raises(ExceptionGroup) as raised:
            await telegram.run_telegram(
                settings, asyncio.Event(), MonitoringProgress(), asyncio.Lock()
            )
        assert "synthetic programming failure" in str(raised.value.exceptions[0])
        assert api.closed

    asyncio.run(scenario())


def test_cancellation_preserves_in_flight_identity_for_explicit_restart_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path)
    failure(store, settings)
    notification_id = store.pending_notifications()[0].id

    async def scenario() -> None:
        api = FakeAPI()
        api.send_started, api.send_block = asyncio.Event(), asyncio.Event()
        fake_boundary(monkeypatch, api)
        stopped = asyncio.Event()
        task = asyncio.create_task(
            telegram.run_telegram(settings, stopped, MonitoringProgress(), asyncio.Lock())
        )
        await asyncio.wait_for(api.send_started.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert api.closed
        pending = store.pending_notifications()
        assert len(pending) == 1 and pending[0].in_flight
        assert pending[0].id == notification_id
        assert pending[0].attempts == 1
        assert store.configure_telegram(CONFIG.destination, START + timedelta(seconds=1)) == 1
        pending = store.pending_notifications()
        assert not pending[0].in_flight
        assert pending[0].last_error == "interrupted"
        assert pending[0].id == notification_id

    asyncio.run(scenario())


def test_shared_rate_limit_delays_both_incident_and_reply_and_persists_pause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path, with_site=False)

    async def scenario() -> None:
        api = FakeAPI()
        api.send_results.extend(
            [TelegramError(TelegramErrorKind.RATE_LIMITED, retry_after=2, status=429), 2]
        )
        stopped = asyncio.Event()
        progress = MonitoringProgress()
        sender = telegram._Sender(api, CONFIG, store, asyncio.Lock(), stopped, progress)
        clock = [100.0]
        monkeypatch.setattr(telegram.time, "monotonic", lambda: clock[0])
        monkeypatch.setattr(telegram, "SEND_INTERVAL", 0)
        with pytest.raises(TelegramError):
            await sender.send("incident", 7, "delivery")
        saved = store.get_send_pause()
        assert saved is not None and saved[1] == 2

        pauses = []

        async def advance(delay: float, *args: object) -> None:
            pauses.append(delay)
            clock[0] += delay

        monkeypatch.setattr(telegram, "_pause", advance)
        assert await sender.send("reply", 7, "replies") == 2
        assert clock[0] == 102
        assert sum(pauses) == 2
        assert [item[1] for item in api.sent] == ["incident", "reply"]
        assert store.get_send_pause() is None

    asyncio.run(scenario())


@pytest.mark.parametrize("wall_jump", [-86400, 86400])
def test_reminder_deadline_uses_monotonic_elapsed_time_despite_wall_clock_jump(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, wall_jump: int
) -> None:
    settings, store = prepare(tmp_path)
    settings = replace(settings, sites=(replace(settings.sites[0], reminder_interval=10),))
    failure(store, settings)
    record = store.reserve_due_delivery(START)
    assert record is not None
    store.acknowledge(record.id, START)

    class Clock:
        @staticmethod
        def now(zone: object) -> datetime:
            return START + timedelta(seconds=wall_jump)

    async def scenario() -> None:
        stopped = asyncio.Event()
        progress = MonitoringProgress(observations={"primary": 100})
        clock = [100.0]
        deadlines: dict[str, tuple[str, datetime, float]] = {}
        monkeypatch.setattr(telegram.time, "monotonic", lambda: clock[0])
        monkeypatch.setattr(telegram, "datetime", Clock)
        iterations = 0

        async def advance(delay: float, *args: object) -> None:
            nonlocal iterations
            iterations += 1
            if iterations == 1:
                assert store.pending_count() == 0
                assert deadlines["primary"][2] == 110
                clock[0] = 109.999
            elif iterations == 2:
                assert store.pending_count() == 0
                clock[0] = 110
            else:
                stopped.set()

        monkeypatch.setattr(telegram, "_pause", advance)
        await telegram._reminder_worker(
            settings, store, asyncio.Lock(), stopped, progress, deadlines
        )
        pending = store.pending_notifications()
        assert len(pending) == 1
        assert pending[0].event.kind is EventKind.REMINDER
        assert pending[0].event.occurred_at == Clock.now(UTC)

    asyncio.run(scenario())


@pytest.mark.parametrize("last_observation", [None, 0.0])
def test_due_reminders_require_current_run_fresh_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, last_observation: float | None
) -> None:
    settings, store = prepare(tmp_path)
    failure(store, settings)
    record = store.reserve_due_delivery(START)
    assert record is not None
    store.acknowledge(record.id, START)
    progress = MonitoringProgress(
        observations={} if last_observation is None else {"primary": last_observation}
    )
    deadlines = {"primary": (record.event.incident_id, START, 99.0)}

    async def scenario() -> None:
        stopped = asyncio.Event()
        monkeypatch.setattr(telegram.time, "monotonic", lambda: 100.0)

        async def finish(*args: object) -> None:
            stopped.set()

        monkeypatch.setattr(telegram, "_pause", finish)
        await telegram._reminder_worker(
            settings, store, asyncio.Lock(), stopped, progress, deadlines
        )
        assert store.pending_count() == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("checkpoint_age", [86400, 86401, -1])
def test_expired_or_future_checkpoint_accepts_randomized_lower_update_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, checkpoint_age: int
) -> None:
    settings, store = prepare(tmp_path, with_site=False)
    store.set_poll_cursor(123456, 900, received_at=START - timedelta(seconds=checkpoint_age))

    class Clock:
        @staticmethod
        def now(zone: object) -> datetime:
            return START

    monkeypatch.setattr(telegram, "datetime", Clock)

    async def scenario() -> None:
        api = FakeAPI()
        fake_boundary(monkeypatch, api)
        api.updates.put_nowait((update(1),))
        stopped = asyncio.Event()
        task = asyncio.create_task(
            telegram.run_telegram(settings, stopped, MonitoringProgress(), asyncio.Lock())
        )
        try:
            await eventually(lambda: bool(api.accepted) and store.get_poll_cursor(123456) == 2)
            assert api.offsets[0] == 0
        finally:
            stopped.set()
            await asyncio.wait_for(task, 2)

    asyncio.run(scenario())


def test_poll_batch_is_processed_in_id_order_without_losing_middle_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path, with_site=False)

    async def scenario() -> None:
        api = FakeAPI()
        fake_boundary(monkeypatch, api)
        api.updates.put_nowait((update(12, user=2), update(10, user=2), update(11)))
        stopped = asyncio.Event()
        task = asyncio.create_task(
            telegram.run_telegram(settings, stopped, MonitoringProgress(), asyncio.Lock())
        )
        try:
            await eventually(lambda: bool(api.accepted) and store.get_poll_cursor(123456) == 13)
            assert len(api.accepted) == 1
        finally:
            stopped.set()
            await asyncio.wait_for(task, 2)

    asyncio.run(scenario())


def test_retry_attempt_budget_becomes_terminal_and_never_retries_forever(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path)
    failure(store, settings)
    monkeypatch.setattr(telegram, "MAX_ATTEMPTS", 2)
    monkeypatch.setattr(telegram, "_retry_delay", lambda attempt, error: 0)

    async def scenario() -> None:
        api = FakeAPI()
        api.send_results.extend([TelegramError(TelegramErrorKind.TRANSIENT) for _ in range(5)])
        fake_boundary(monkeypatch, api)
        stopped = asyncio.Event()
        task = asyncio.create_task(
            telegram.run_telegram(settings, stopped, MonitoringProgress(), asyncio.Lock())
        )
        try:
            await eventually(lambda: store.delivery_status()["failed"] == 1)
            assert len(api.sent) == 2
            assert store.pending_count() == 0
            assert store.delivery_status()["last_error"] == "retries_exhausted"
        finally:
            stopped.set()
            await asyncio.wait_for(task, 2)

    asyncio.run(scenario())


def test_restored_shared_pause_repeats_saved_duration_without_trusting_wall_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, store = prepare(tmp_path, with_site=False)
    store.set_send_pause(START - timedelta(days=30), 5)

    async def scenario() -> None:
        api = FakeAPI()
        stopped = asyncio.Event()
        sender = telegram._Sender(api, CONFIG, store, asyncio.Lock(), stopped, MonitoringProgress())
        clock = [100.0]
        monkeypatch.setattr(telegram.time, "monotonic", lambda: clock[0])

        async def advance(delay: float, *args: object) -> None:
            clock[0] += delay

        monkeypatch.setattr(telegram, "_pause", advance)
        await sender.restore_pause()
        assert await sender.send("reply", 7, "replies") == 1
        assert clock[0] == 105
        assert store.get_send_pause() is None

    asyncio.run(scenario())


def test_continuous_reply_backlog_does_not_starve_incident_delivery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path)
    failure(store, settings)
    monkeypatch.setattr(telegram, "SEND_INTERVAL", 0.005)

    async def scenario() -> None:
        api = FakeAPI()
        stopped = asyncio.Event()
        progress = MonitoringProgress()
        lock = asyncio.Lock()
        sender = telegram._Sender(api, CONFIG, store, lock, stopped, progress)
        replies: asyncio.Queue[telegram._Reply] = asyncio.Queue(maxsize=20)
        for _ in range(20):
            replies.put_nowait(telegram._Reply("cached reply", 7))

        async def replenish() -> None:
            while not stopped.is_set():
                await replies.put(telegram._Reply("cached reply", 7))

        tasks = [
            asyncio.create_task(
                telegram._reply_worker(sender, replies, store, lock, stopped, progress)
            ),
            asyncio.create_task(replenish()),
        ]
        await eventually(lambda: bool(api.sent))
        tasks.append(
            asyncio.create_task(
                telegram._deliver(sender, settings, store, lock, stopped, progress, {})
            )
        )
        try:
            await eventually(lambda: store.delivery_status()["accepted"] == 1)
            assert any(item[1].startswith("DOWN:") for item in api.accepted)
        finally:
            stopped.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(scenario())


def test_cancelling_send_waiting_for_owner_does_not_leak_the_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, store = prepare(tmp_path, with_site=False)
    monkeypatch.setattr(telegram, "SEND_INTERVAL", 0)

    async def scenario() -> None:
        api = FakeAPI()
        api.send_started, api.send_block = asyncio.Event(), asyncio.Event()
        progress = MonitoringProgress()
        sender = telegram._Sender(api, CONFIG, store, asyncio.Lock(), asyncio.Event(), progress)
        owner = asyncio.create_task(sender.send("owner", 7, "replies"))
        await asyncio.wait_for(api.send_started.wait(), 2)
        waiting = asyncio.create_task(sender.send("cancelled", 7, "delivery"))
        await eventually(lambda: "delivery" in progress.telegram)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        api.send_block.set()
        await asyncio.wait_for(owner, 2)
        assert await asyncio.wait_for(sender.send("after cancellation", 7, "delivery"), 2) == 2
        assert [message[1] for message in api.accepted] == ["owner", "after cancellation"]

    asyncio.run(scenario())


def test_sends_queued_behind_current_owner_keep_fifo_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, store = prepare(tmp_path, with_site=False)
    monkeypatch.setattr(telegram, "SEND_INTERVAL", 0)

    async def scenario() -> None:
        api = FakeAPI()
        api.send_started, api.send_block = asyncio.Event(), asyncio.Event()
        progress = MonitoringProgress()
        sender = telegram._Sender(api, CONFIG, store, asyncio.Lock(), asyncio.Event(), progress)
        first = asyncio.create_task(sender.send("first", 7, "replies"))
        await asyncio.wait_for(api.send_started.wait(), 2)
        second = asyncio.create_task(sender.send("second", 7, "delivery"))
        await eventually(lambda: "delivery" in progress.telegram)
        third = asyncio.create_task(sender.send("third", 7, "reminders"))
        await eventually(lambda: "reminders" in progress.telegram)
        assert [message[1] for message in api.sent] == ["first"]
        api.send_block.set()
        assert await asyncio.wait_for(asyncio.gather(first, second, third), 2) == [1, 2, 3]
        assert [message[1] for message in api.accepted] == ["first", "second", "third"]

    asyncio.run(scenario())


def test_polling_retry_after_cools_down_incident_and_reply_sends(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path, with_site=False)
    monkeypatch.setattr(telegram, "SEND_INTERVAL", 0)

    async def scenario() -> None:
        clock = [100.0]
        send_times: list[float] = []

        class TimedAPI(FakeAPI):
            async def send_message(self, *args: object, **kwargs: object) -> int:
                send_times.append(clock[0])
                return await super().send_message(*args, **kwargs)

        api = TimedAPI()
        api.updates.put_nowait(
            TelegramError(TelegramErrorKind.RATE_LIMITED, retry_after=2, status=429)
        )
        stopped = asyncio.Event()
        progress = MonitoringProgress()
        lock = asyncio.Lock()
        sender = telegram._Sender(api, CONFIG, store, lock, stopped, progress)
        poll_paused, hold_poll = asyncio.Event(), asyncio.Event()
        monkeypatch.setattr(telegram.time, "monotonic", lambda: clock[0])

        async def advance(delay: float, stop: object, state: object, worker: str | None) -> None:
            if worker == "poll":
                poll_paused.set()
                await hold_poll.wait()
            else:
                assert api.sent == []
                clock[0] += delay

        monkeypatch.setattr(telegram, "_pause", advance)
        poll = asyncio.create_task(
            telegram._poll(
                api,
                BotIdentity(123456, "Sentinel_Test_Bot"),
                settings,
                store,
                lock,
                stopped,
                progress,
                asyncio.Queue(maxsize=20),
                sender,
            )
        )
        try:
            await poll_paused.wait()
            saved = store.get_send_pause()
            assert saved is not None and saved[1] == 2
            await asyncio.gather(
                sender.send("incident", 7, "delivery"), sender.send("reply", 7, "replies")
            )
            assert send_times == [102.0, 102.0]
            assert store.get_send_pause() is None
        finally:
            poll.cancel()
            await asyncio.gather(poll, return_exceptions=True)

    asyncio.run(scenario())


def test_later_rate_hint_extends_existing_pause_before_sends_can_clear_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, store = prepare(tmp_path, with_site=False)
    monkeypatch.setattr(telegram, "SEND_INTERVAL", 0)

    async def scenario() -> None:
        api = FakeAPI()
        clock = [100.0]
        sender = telegram._Sender(
            api, CONFIG, store, asyncio.Lock(), asyncio.Event(), MonitoringProgress()
        )
        monkeypatch.setattr(telegram.time, "monotonic", lambda: clock[0])
        await sender.rate_limited(TelegramError(TelegramErrorKind.RATE_LIMITED, retry_after=2))
        pauses: list[float] = []

        async def advance(delay: float, *args: object) -> None:
            pauses.append(delay)
            assert api.sent == []
            clock[0] += delay
            if len(pauses) == 1:
                await sender.rate_limited(
                    TelegramError(TelegramErrorKind.RATE_LIMITED, retry_after=10)
                )
            else:
                saved = store.get_send_pause()
                assert saved is not None and saved[1] == 10

        monkeypatch.setattr(telegram, "_pause", advance)
        assert await sender.send("after both pauses", 7, "delivery") == 1
        assert pauses == [2.0, 10.0]
        assert clock[0] == 112
        assert store.get_send_pause() is None

    asyncio.run(scenario())
