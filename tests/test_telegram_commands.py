from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from sentinel_bot.config import TelegramSettings
from sentinel_bot.telegram_commands import (
    MAX_MESSAGE_UNITS,
    Command,
    CommandGate,
    IncomingMessage,
    bound_text,
    parse_message,
    render_command,
)

NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)
SETTINGS = TelegramSettings(
    token="synthetic-token", chat_id=-100, allowed_user_ids=frozenset({1, 2, 3}), topic_id=7
)
MESSAGE = IncomingMessage(-100, 1, 7, "/status", False, False)


def update() -> dict[str, object]:
    return {
        "update_id": 10,
        "message": {
            "message_id": 11,
            "chat": {"id": -100, "type": "supergroup"},
            "from": {"id": 1, "is_bot": False},
            "message_thread_id": 7,
            "text": "/status",
        },
    }


def snapshot() -> dict[str, object]:
    return {
        "mode": "monitor",
        "pending_notifications": 3,
        "sites": [
            {
                "alias": "primary",
                "state": "DOWN",
                "fresh": True,
                "incident_id": "synthetic-incident",
                "observation": {
                    "checked_at": (NOW - timedelta(seconds=5)).isoformat(),
                    "success": False,
                    "error": "http_status",
                    "http_status": 503,
                    "latency_ms": 12.5,
                },
            }
        ],
    }


@pytest.mark.parametrize("chat_type", ["private", "group", "supergroup"])
def test_parse_only_normal_user_messages(chat_type: str) -> None:
    raw = update()
    raw["message"]["chat"]["type"] = chat_type
    assert parse_message(raw) == MESSAGE


@pytest.mark.parametrize(
    "value",
    [None, 42, "message", [], {}, {"chat": [], "from": []}],
)
def test_malformed_message_shapes_are_ignored(value: object) -> None:
    assert parse_message({"message": value}) is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("chat", {"id": True, "type": "group"}),
        ("chat", {"id": 0, "type": "group"}),
        ("chat", {"id": -100, "type": "channel"}),
        ("chat", {"id": -100, "type": ["group"]}),
        ("chat", {"id": -(2**63), "type": "group"}),
        ("from", None),
        ("from", {"id": 1}),
        ("from", {"id": True, "is_bot": False}),
        ("from", {"id": -1, "is_bot": False}),
        ("from", {"id": 1, "is_bot": 0}),
        ("from", {"id": 1, "is_bot": True}),
        ("sender_chat", {"id": -100}),
        ("sender_chat", None),
        ("message_thread_id", True),
        ("message_thread_id", 0),
        ("message_thread_id", None),
        ("text", None),
        ("text", ""),
        ("text", "/" + "a" * 4096),
        ("text", "😀" * 2049),
    ],
)
def test_parser_rejects_bots_anonymous_senders_and_malformed_fields(
    field: str, value: object
) -> None:
    raw = update()
    raw["message"][field] = value
    assert parse_message(raw) is None


def test_edited_and_channel_posts_do_not_run_commands() -> None:
    raw = update()
    assert parse_message({"edited_message": raw["message"]}) is None
    assert parse_message({"channel_post": raw["message"]}) is None


def test_topic_is_optional_in_messages() -> None:
    raw = update()
    del raw["message"]["message_thread_id"]
    assert parse_message(raw) == replace(MESSAGE, topic_id=None)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("/help", Command("help")),
        ("/status", Command("status")),
        ("/sites", Command("sites")),
        ("/servers", Command("servers")),
        ("/server worker-1", Command("server", "worker-1")),
        ("/status@Sentinel_Test_Bot", Command("status")),
        ("/status@sentinel_test_bot\n", Command("status")),
        ("/server@Sentinel_Test_Bot worker_1", Command("server", "worker_1")),
        ("/unknown", Command("help")),
        ("/server", Command("help")),
        ("/server https://example.test", Command("help")),
        ("/server ../../etc/passwd", Command("help")),
        ("/server one two", Command("help")),
        ("/status synthetic-secret", Command("help")),
        ("/servers;reboot", None),
        ("/status@different_bot", None),
        ("/status@", None),
        ("/123", None),
        ("status", None),
        (" /status", None),
        ("", None),
        ("/status " + "x" * 256, None),
    ],
)
def test_parse_configured_commands_without_arbitrary_targets(
    text: str, expected: Command | None
) -> None:
    gate = CommandGate(SETTINGS, "Sentinel_Test_Bot")
    assert gate.accept(replace(MESSAGE, text=text), now=0) == expected


@pytest.mark.parametrize(
    "changes",
    [
        {"chat_id": -101},
        {"chat_id": True},
        {"user_id": 4},
        {"user_id": None},
        {"user_id": True},
        {"topic_id": 8},
        {"topic_id": None},
        {"is_bot": True},
        {"is_bot": None},
        {"sender_chat": True},
        {"sender_chat": None},
    ],
)
def test_authorization_rejects_wrong_chat_topic_and_actor(changes: dict[str, object]) -> None:
    assert (
        CommandGate(SETTINGS, "Sentinel_Test_Bot").accept(replace(MESSAGE, **changes), now=0)
        is None
    )


def test_optional_topic_accepts_all_topics_only_in_configured_chat() -> None:
    settings = replace(SETTINGS, topic_id=None)
    gate = CommandGate(settings, "Sentinel_Test_Bot")
    assert gate.accept(replace(MESSAGE, topic_id=None), now=0) == Command("status")
    assert gate.accept(replace(MESSAGE, topic_id=123), now=0) == Command("status")
    assert gate.accept(replace(MESSAGE, chat_id=-200), now=10) is None


def test_unauthorized_and_noncommands_do_not_consume_authorized_budgets() -> None:
    gate = CommandGate(SETTINGS, "Sentinel_Test_Bot")
    for identifier in range(100, 1100):
        assert gate.accept(replace(MESSAGE, user_id=identifier), now=0) is None
    for _ in range(20):
        assert gate.accept(replace(MESSAGE, text="regular discussion"), now=0) is None
    assert gate.accept(MESSAGE, now=0) == Command("status")
    assert gate.accept(MESSAGE, now=0) == Command("status")
    assert gate.accept(MESSAGE, now=0) is None


def test_per_user_rate_recovers_at_exact_monotonic_deadline() -> None:
    gate = CommandGate(SETTINGS, "Sentinel_Test_Bot")
    assert gate.accept(MESSAGE, now=100) is not None
    assert gate.accept(MESSAGE, now=100) is not None
    assert gate.accept(MESSAGE, now=109.999) is None
    assert gate.accept(MESSAGE, now=110) is not None
    assert gate.accept(MESSAGE, now=110) is None


def test_global_limit_applies_across_users_without_debiting_denied_user() -> None:
    gate = CommandGate(SETTINGS, "Sentinel_Test_Bot")
    for identifier in (1, 1, 2, 2, 3):
        assert gate.accept(replace(MESSAGE, user_id=identifier), now=100) is not None
    assert gate.accept(replace(MESSAGE, user_id=3), now=101.999) is None
    assert gate.accept(replace(MESSAGE, user_id=3), now=102) is not None


def test_backward_clock_does_not_refill_budget() -> None:
    gate = CommandGate(SETTINGS, "Sentinel_Test_Bot")
    assert gate.accept(MESSAGE, now=100) is not None
    assert gate.accept(MESSAGE, now=100) is not None
    assert gate.accept(MESSAGE, now=90) is None
    assert gate.accept(MESSAGE, now=100) is None
    assert gate.accept(MESSAGE, now=110) is not None


@pytest.mark.parametrize("now", [-1, float("nan"), float("inf")])
def test_invalid_command_clock_is_visible(now: float) -> None:
    with pytest.raises(ValueError, match="time"):
        CommandGate(SETTINGS, "Sentinel_Test_Bot").accept(MESSAGE, now=now)


@pytest.mark.parametrize("username", ["", "bad name", "@bot", "a" * 33])
def test_invalid_bot_username_cannot_create_gate(username: str) -> None:
    with pytest.raises(ValueError, match="username"):
        CommandGate(SETTINGS, username)


@pytest.mark.parametrize("users", [frozenset(), frozenset(range(1, 22))])
def test_gate_user_storage_is_bounded_by_allowlist(users: frozenset[int]) -> None:
    with pytest.raises(ValueError, match="allowlist"):
        CommandGate(replace(SETTINGS, allowed_user_ids=users), "Sentinel_Test_Bot")


@pytest.mark.parametrize("name", ["status", "sites"])
def test_cached_views_show_confirmed_state_current_failure_age_and_backlog(name: str) -> None:
    text = render_command(Command(name), snapshot(), now=NOW)
    assert "Snapshot time (UTC): 2026-01-01T12:00:00+00:00" in text
    assert "Pending notifications: 3" in text
    assert "primary: DOWN; fresh; failed/http_status HTTP503 12ms; age=5s" in text


def test_stale_confirmed_up_is_not_presented_as_current_healthy_reading() -> None:
    cached = snapshot()
    cached["sites"][0].update(state="UP", fresh=False)
    cached["sites"][0]["observation"].update(success=True, error=None, http_status=200)
    text = render_command(Command("status"), cached, now=NOW)
    assert "UP; stale; passed HTTP200" in text


def test_unknown_observation_does_not_invent_zero_latency_or_check_age() -> None:
    cached = snapshot()
    cached["sites"][0].update(state="UNKNOWN", fresh=False, observation=None)
    text = render_command(Command("sites"), cached, now=NOW)
    assert "UNKNOWN; stale; unknown; age=unknown" in text
    assert "0ms" not in text


@pytest.mark.parametrize(
    ("seconds", "age"),
    [(-1, "clock-skew"), (0, "0s"), (59, "59s"), (60, "1m"), (3600, "1h"), (86400, "1d")],
)
def test_check_age_units_and_future_clock_are_explicit(seconds: int, age: str) -> None:
    cached = snapshot()
    cached["sites"][0]["observation"]["checked_at"] = (NOW - timedelta(seconds=seconds)).isoformat()
    assert f"age={age}" in render_command(Command("sites"), cached, now=NOW)


@pytest.mark.parametrize("timestamp", [None, "not-a-date", "2026-01-01T12:00:00", 123])
def test_missing_or_invalid_timestamps_remain_unknown(timestamp: object) -> None:
    cached = snapshot()
    cached["sites"][0]["observation"]["checked_at"] = timestamp
    assert "age=unknown" in render_command(Command("sites"), cached, now=NOW)


def test_equivalent_timezone_has_same_age() -> None:
    assert "age=5s" in render_command(
        Command("sites"), snapshot(), now=NOW.astimezone(timezone(timedelta(hours=4)))
    )


def test_naive_render_clock_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone"):
        render_command(Command("status"), snapshot(), now=NOW.replace(tzinfo=None))


@pytest.mark.parametrize("name", ["help", "unknown"])
def test_help_does_not_echo_arguments_or_snapshot_secrets(name: str) -> None:
    text = render_command(Command(name, "synthetic-secret"), {"token": "synthetic-secret"}, now=NOW)
    assert all(command in text for command in ("/help", "/status", "/sites", "/servers", "/server"))
    assert "synthetic-secret" not in text
    assert "cached" in text


@pytest.mark.parametrize("name", ["servers", "server"])
def test_servers_are_honestly_unconfigured_and_arbitrary_alias_is_not_echoed(name: str) -> None:
    text = render_command(Command(name, "https://private.test/secret"), snapshot(), now=NOW)
    assert "No server sources configured" in text
    assert "private.test" not in text


def test_no_sources_and_missing_backlog_are_explicit() -> None:
    text = render_command(Command("status"), {}, now=NOW)
    assert "No website sources configured" in text
    assert "Pending notifications: unknown" in text


def test_delivery_view_distinguishes_pending_retrying_failed_and_blocked() -> None:
    cached = snapshot()
    cached["telegram"] = {
        "pending": 2,
        "retrying": 1,
        "failed": 3,
        "blocked": 1,
        "last_error": "timeout",
    }
    text = render_command(Command("status"), cached, now=NOW)
    assert "Telegram: pending=2; retrying=1; failed=3; blocked=1" in text
    assert "Telegram error: timeout" in text


@pytest.mark.parametrize(
    "category",
    [
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
    ],
)
def test_normalized_delivery_error_categories_are_visible(category: str) -> None:
    cached = snapshot()
    cached["telegram"] = {"last_error": category}
    assert f"Telegram error: {category}" in render_command(Command("status"), cached, now=NOW)


def test_transport_errors_and_extra_fields_never_expose_raw_credentials_or_urls() -> None:
    cached = snapshot()
    cached["token"] = "synthetic-secret"
    cached["url"] = "https://private.test"
    cached["telegram"] = {"last_error": "synthetic-secret", "pending": 1}
    cached["sites"][0]["observation"]["error"] = "https://private.test/synthetic-secret"
    text = render_command(Command("status"), cached, now=NOW)
    assert "synthetic-secret" not in text
    assert "private.test" not in text
    assert "failed HTTP503" in text


def test_largest_inventory_is_bounded_and_last_alias_remains_visible() -> None:
    cached = snapshot()
    site = cached["sites"][0]
    cached["sites"] = [dict(site, alias=f"site{index:02}" + "x" * 26) for index in range(20)]
    text = render_command(Command("status"), cached, now=NOW)
    assert "site19" in text
    assert len(text.encode("utf-16-le")) // 2 <= MAX_MESSAGE_UNITS


@pytest.mark.parametrize(
    ("text", "limit", "expected"),
    [
        ("normal", 6, "normal"),
        ("normal", 5, "norm…"),
        ("😀😀", 4, "😀😀"),
        ("😀😀", 3, "😀…"),
        ("😀😀", 2, "…"),
        ("😀", 1, "…"),
        ("line\nnext\r\x00\x1b\u202eevil\udfff", 100, "line\nnextevil"),
    ],
)
def test_utf16_bound_never_splits_emoji_and_strips_control_characters(
    text: str, limit: int, expected: str
) -> None:
    result = bound_text(text, limit)
    assert result == expected
    assert len(result.encode("utf-16-le")) // 2 <= limit


def test_message_limit_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        bound_text("message", 0)
