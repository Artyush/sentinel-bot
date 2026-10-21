import asyncio
import importlib.util
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web

from sentinel_bot.telegram_api import BotIdentity, TelegramAPI, TelegramError, TelegramErrorKind

SPEC = importlib.util.spec_from_file_location(
    "demo_telegram_fixture", Path(__file__).resolve().parents[1] / "demo/telegram_fixture.py"
)
assert SPEC is not None and SPEC.loader is not None
telegram_fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(telegram_fixture)
BOT_ID = telegram_fixture.BOT_ID
BOT_USERNAME = telegram_fixture.BOT_USERNAME
CHAT_ID = telegram_fixture.CHAT_ID
MAX_ACTIVE_REQUESTS = telegram_fixture.MAX_ACTIVE_REQUESTS
MAX_BODY_BYTES = telegram_fixture.MAX_BODY_BYTES
MAX_MESSAGES = telegram_fixture.MAX_MESSAGES
MAX_UPDATES = telegram_fixture.MAX_UPDATES
TOKEN = telegram_fixture.TOKEN
USER_ID = telegram_fixture.USER_ID
applications = telegram_fixture.applications


@asynccontextmanager
async def fixture() -> AsyncIterator[tuple[str, str, aiohttp.ClientSession]]:
    runners: list[web.AppRunner] = []
    origins: list[str] = []
    try:
        for app in applications():
            runner = web.AppRunner(app, access_log=None, handler_cancellation=True)
            await runner.setup()
            runners.append(runner)
            await web.TCPSite(runner, "127.0.0.1", 0).start()
            origins.append(f"http://127.0.0.1:{runner.addresses[0][1]}")
        async with aiohttp.ClientSession() as client:
            yield origins[0], origins[1], client
    finally:
        for runner in reversed(runners):
            await runner.cleanup()


def test_real_adapter_startup_and_send_are_visible_in_read_only_inbox() -> None:
    async def scenario() -> None:
        async with fixture() as (origin, ui, client):
            async with TelegramAPI(TOKEN, fixture_base_url=origin) as api:
                assert await api.startup() == BotIdentity(BOT_ID, BOT_USERNAME)
                assert await api.send_message(CHAT_ID, "Synthetic DOWN", topic_id=7) == 1
                assert await api.send_message(CHAT_ID, "Synthetic RECOVERY") == 2
            async with client.get(f"{ui}/api/messages") as response:
                assert response.status == 200
                state = await response.json()
            assert state["startup_count"] == 1
            assert state["accepted_count"] == state["send_attempts"] == 2
            assert state["failed_attempts"] == 0
            assert state["messages"][0]["message_thread_id"] == 7
            assert state["messages"][1]["text"] == "Synthetic RECOVERY"
            assert TOKEN not in str(state)
            async with client.get(f"{ui}/health") as response:
                assert response.status == 200
                assert await response.text() == "SYNTHETIC"

    asyncio.run(scenario())


def test_outage_rejects_sends_but_preserves_polling_health_and_accepted_history() -> None:
    async def scenario() -> None:
        async with fixture() as (origin, ui, client):
            async with TelegramAPI(TOKEN, fixture_base_url=origin) as api:
                await api.send_message(CHAT_ID, "Before outage")
                async with client.post(f"{origin}/control", data="down") as response:
                    assert response.status == 200
                with pytest.raises(TelegramError) as caught:
                    await api.send_message(CHAT_ID, "Retry later")
                assert caught.value.kind is TelegramErrorKind.TRANSIENT
                assert await api.startup() == BotIdentity(BOT_ID, BOT_USERNAME)
                assert await api.get_updates(None, timeout=0) == ()
                async with client.get(f"{ui}/health") as response:
                    assert response.status == 200
                async with client.get(f"{ui}/api/messages") as response:
                    state = await response.json()
                assert state["mode"] == "down"
                assert state["accepted_count"] == state["failed_attempts"] == 1
                assert state["send_attempts"] == 2
                async with client.post(f"{origin}/control", data="up") as response:
                    assert response.status == 200
                assert await api.send_message(CHAT_ID, "Retry later") == 2

    asyncio.run(scenario())


def test_command_updates_are_replayed_until_acknowledged_and_queue_is_bounded() -> None:
    async def scenario() -> None:
        async with fixture() as (origin, _, client):
            for number in range(MAX_UPDATES):
                async with client.post(
                    f"{origin}/command", json={"text": f"/server node{number}"}
                ) as response:
                    assert response.status == 202
                    assert await response.json() == {"update_id": number + 1}
            async with client.post(f"{origin}/command", json={"text": "/status"}) as response:
                assert response.status == 429
            async with TelegramAPI(TOKEN, fixture_base_url=origin) as api:
                updates = await api.get_updates(None, timeout=0)
                assert len(updates) == MAX_UPDATES
                assert updates == await api.get_updates(1, timeout=0)
                message = updates[0]["message"]
                assert message["chat"]["id"] == CHAT_ID
                assert message["from"]["id"] == USER_ID
                assert message["from"]["is_bot"] is False
                assert await api.get_updates(MAX_UPDATES + 1, timeout=0) == ()
                async with client.post(
                    f"{origin}/command",
                    json={"text": "/status", "user_id": 999, "message_thread_id": 7},
                ) as response:
                    assert response.status == 202
                updates = await api.get_updates(MAX_UPDATES + 1, timeout=0)
                assert updates[0]["message"]["from"]["id"] == 999
                assert updates[0]["message"]["message_thread_id"] == 7

    asyncio.run(scenario())


def test_one_long_poll_waits_for_command_and_cancellation_releases_polling_slot() -> None:
    async def wait_until_polling(origin: str, client: aiohttp.ClientSession) -> None:
        async with asyncio.timeout(2):
            while True:
                async with client.post(
                    f"{origin}/bot{TOKEN}/getUpdates", json={"timeout": 0}
                ) as response:
                    if response.status == 409:
                        return
                await asyncio.sleep(0.01)

    async def scenario() -> None:
        async with fixture() as (origin, _, client):
            async with TelegramAPI(TOKEN, fixture_base_url=origin) as api:
                polling = asyncio.create_task(api.get_updates(None, timeout=20))
                try:
                    await wait_until_polling(origin, client)
                    async with client.post(
                        f"{origin}/command", json={"text": "/status"}
                    ) as response:
                        assert response.status == 202
                    updates = await asyncio.wait_for(polling, 1)
                    assert updates[0]["message"]["text"] == "/status"
                finally:
                    polling.cancel()
                    with suppress(asyncio.CancelledError):
                        await polling
                polling = asyncio.create_task(api.get_updates(2, timeout=20))
                try:
                    await wait_until_polling(origin, client)
                finally:
                    polling.cancel()
                    with suppress(asyncio.CancelledError):
                        await polling
                async with asyncio.timeout(2):
                    while True:
                        try:
                            assert await api.get_updates(2, timeout=0) == ()
                            break
                        except TelegramError as error:
                            assert error.kind is TelegramErrorKind.CONFLICT
                            await asyncio.sleep(0.01)

    asyncio.run(scenario())


def test_retained_history_is_bounded_and_inbox_escapes_message_content() -> None:
    async def scenario() -> None:
        async with fixture() as (origin, ui, client):
            async with TelegramAPI(TOKEN, fixture_base_url=origin) as api:
                for number in range(MAX_MESSAGES + 3):
                    assert (
                        await api.send_message(CHAT_ID, f"<script>{number}</script>") == number + 1
                    )
            async with client.get(f"{ui}/api/messages") as response:
                state = await response.json()
            assert state["accepted_count"] == MAX_MESSAGES + 3
            assert len(state["messages"]) == MAX_MESSAGES
            assert state["messages"][0]["message_id"] == 4
            async with client.get(ui) as response:
                body = await response.text()
                assert "<script>" not in body
                assert "&lt;script&gt;202&lt;/script&gt;" in body
                assert "frame-ancestors 'none'" in response.headers["Content-Security-Policy"]
                assert response.headers["Cache-Control"] == "no-store"

    asyncio.run(scenario())


@pytest.mark.parametrize("path", ["/control", "/command", f"/bot{TOKEN}/sendMessage"])
def test_published_inbox_app_has_no_mutating_routes(path: str) -> None:
    async def scenario() -> None:
        async with fixture() as (_, ui, client):
            async with client.post(f"{ui}{path}", json={"text": "Synthetic"}) as response:
                assert response.status == 404

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("method", "body"),
    [
        ("sendMessage", {"chat_id": CHAT_ID + 1, "text": "Wrong destination"}),
        ("sendMessage", {"chat_id": True, "text": "Wrong destination"}),
        ("sendMessage", {"chat_id": CHAT_ID, "text": ""}),
        ("sendMessage", {"chat_id": CHAT_ID, "text": "😀" * 2049}),
        ("sendMessage", {"chat_id": CHAT_ID, "text": "\ud800"}),
        ("sendMessage", {"chat_id": CHAT_ID, "text": "x", "message_thread_id": True}),
        ("sendMessage", {"chat_id": CHAT_ID, "text": "x", "message_thread_id": 2**31}),
        ("getUpdates", {"offset": -1}),
        ("getUpdates", {"offset": True}),
        ("getUpdates", {"limit": MAX_UPDATES + 1}),
        ("getUpdates", {"timeout": 31}),
        ("getUpdates", {"allowed_updates": ["channel_post"]}),
        ("getUpdates", {"unknown": "value"}),
    ],
)
def test_synthetic_api_rejects_invalid_contract(method: str, body: object) -> None:
    async def scenario() -> None:
        async with fixture() as (origin, _, client):
            async with client.post(f"{origin}/bot{TOKEN}/{method}", json=body) as response:
                assert response.status == 400

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "body",
    [
        [],
        {},
        {"text": "x" * 257},
        {"text": "\ud800"},
        {"text": "/status", "user_id": False},
        {"text": "/status", "chat_id": 0},
        {"text": "/status", "message_thread_id": None},
        {"text": "/status", "url": "https://example.test"},
    ],
)
def test_synthetic_command_input_is_strict_and_bounded(body: object) -> None:
    async def scenario() -> None:
        async with fixture() as (origin, _, client):
            async with client.post(f"{origin}/command", json=body) as response:
                assert response.status == 400

    asyncio.run(scenario())


def test_bad_token_unsupported_methods_media_types_and_oversized_body_are_rejected() -> None:
    async def scenario() -> None:
        async with fixture() as (origin, _, client):
            async with client.post(f"{origin}/botwrong/getMe", json={}) as response:
                assert response.status == 401
            async with client.post(f"{origin}/bot{TOKEN}/deleteWebhook", json={}) as response:
                assert response.status == 404
            async with client.post(f"{origin}/command", data='{"text":"/status"}') as response:
                assert response.status == 415
            async with client.post(
                f"{origin}/command", data="{", headers={"Content-Type": "application/json"}
            ) as response:
                assert response.status == 400
            async with client.post(
                f"{origin}/command",
                data=b" " * (MAX_BODY_BYTES + 1),
                headers={"Content-Type": "application/json"},
            ) as response:
                assert response.status == 413
            async with client.post(f"{origin}/control", data="invalid") as response:
                assert response.status == 400

    asyncio.run(scenario())


def test_slow_request_bodies_have_a_deadline_and_a_shared_concurrency_limit() -> None:
    async def scenario() -> None:
        async with fixture() as (origin, ui, client):
            release = asyncio.Event()

            async def body() -> AsyncIterator[bytes]:
                yield b'{"text":'
                await release.wait()
                yield b'"/status"}'

            async def submit() -> int:
                async with client.post(
                    f"{origin}/command",
                    data=body(),
                    headers={"Content-Type": "application/json"},
                ) as response:
                    await response.read()
                    return response.status

            requests = [asyncio.create_task(submit()) for _ in range(MAX_ACTIVE_REQUESTS)]
            try:
                async with asyncio.timeout(1):
                    while True:
                        async with client.get(f"{ui}/health") as response:
                            if response.status == 429:
                                break
                        await asyncio.sleep(0.01)
                assert (
                    await asyncio.wait_for(asyncio.gather(*requests), 3)
                    == [408] * MAX_ACTIVE_REQUESTS
                )
                async with client.get(f"{ui}/health") as response:
                    assert response.status == 200
            finally:
                release.set()
                for task in requests:
                    task.cancel()
                await asyncio.gather(*requests, return_exceptions=True)

    asyncio.run(scenario())
