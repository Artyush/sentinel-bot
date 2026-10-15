import asyncio
import traceback
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from sentinel_bot.telegram_api import (
    MAX_RESPONSE_BYTES,
    MAX_RETRY_AFTER,
    MAX_UPDATES,
    BotIdentity,
    TelegramAPI,
    TelegramError,
    TelegramErrorKind,
)

TOKEN = "123456:" + "synthetic_test_credential_only_123456"


@asynccontextmanager
async def bot_fixture(
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> AsyncIterator[str]:
    app = web.Application()
    app.router.add_post("/{path:.*}", handler)
    runner = web.AppRunner(app, shutdown_timeout=0.1)
    await runner.setup()
    server = web.TCPSite(runner, "127.0.0.1", 0)
    await server.start()
    try:
        yield f"http://127.0.0.1:{runner.addresses[0][1]}"
    finally:
        await runner.cleanup()


def success(result: object) -> web.Response:
    return web.json_response({"ok": True, "result": result})


def test_startup_checks_identity_and_webhook_without_sending() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, object]] = []

        async def respond(request: web.Request) -> web.Response:
            calls.append((request.path, await request.json()))
            if request.path.endswith("/getMe"):
                return success({"id": 123456, "is_bot": True, "username": "SyntheticBot"})
            return success({"url": ""})

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                assert await api.startup() == BotIdentity(123456, "SyntheticBot")
        assert calls == [(f"/bot{TOKEN}/getMe", {}), (f"/bot{TOKEN}/getWebhookInfo", {})]

    asyncio.run(scenario())


def test_existing_webhook_is_a_fatal_configuration_error_without_deletion() -> None:
    async def scenario() -> None:
        calls: list[str] = []

        async def respond(request: web.Request) -> web.Response:
            calls.append(request.path.rsplit("/", 1)[-1])
            if calls[-1] == "getMe":
                return success({"id": 123456, "is_bot": True, "username": "SyntheticBot"})
            return success({"url": "https://private.example.test/secret-webhook"})

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.startup()
        assert caught.value.kind is TelegramErrorKind.WEBHOOK
        assert not caught.value.retryable
        assert "private" not in str(caught.value)
        assert calls == ["getMe", "getWebhookInfo"]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "identity",
    [
        None,
        [],
        {},
        {"id": True, "is_bot": True, "username": "SyntheticBot"},
        {"id": 0, "is_bot": True, "username": "SyntheticBot"},
        {"id": 2**52, "is_bot": True, "username": "SyntheticBot"},
        {"id": 1, "is_bot": False, "username": "SyntheticBot"},
        {"id": 1, "is_bot": True, "username": 123},
        {"id": 1, "is_bot": True, "username": "private\nname"},
        {"id": 1, "is_bot": True, "username": "a" * 33},
    ],
)
def test_malformed_identity_is_not_accepted(identity: object) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return success(identity)

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.startup()
        assert caught.value.kind is TelegramErrorKind.PROTOCOL

    asyncio.run(scenario())


@pytest.mark.parametrize("webhook", [None, [], {}, {"url": False}])
def test_missing_or_malformed_webhook_state_is_not_assumed_empty(webhook: object) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            if request.path.endswith("/getMe"):
                return success({"id": 123456, "is_bot": True, "username": "SyntheticBot"})
            return success(webhook)

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.startup()
        assert caught.value.kind is TelegramErrorKind.PROTOCOL

    asyncio.run(scenario())


def test_long_poll_uses_explicit_offset_limit_and_allowed_message_updates() -> None:
    async def scenario() -> None:
        parameters: list[object] = []

        async def respond(request: web.Request) -> web.Response:
            parameters.append(await request.json())
            return success([{"update_id": 42, "message": {"text": "/status"}}])

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                assert await api.get_updates(42, timeout=0) == (
                    {"update_id": 42, "message": {"text": "/status"}},
                )
                await api.get_updates(None)
        assert parameters == [
            {"offset": 42, "limit": 20, "timeout": 0, "allowed_updates": ["message"]},
            {"limit": 20, "timeout": 20, "allowed_updates": ["message"]},
        ]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "updates",
    [
        {},
        None,
        [None],
        [{}],
        [{"update_id": True}],
        [{"update_id": -1}],
        [{"update_id": 2**52}],
        [{"update_id": 1}] * (MAX_UPDATES + 1),
    ],
)
def test_malformed_or_oversized_update_batch_is_rejected(updates: object) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return success(updates)

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.get_updates(None, timeout=0)
        assert caught.value.kind is TelegramErrorKind.PROTOCOL

    asyncio.run(scenario())


def test_plain_text_send_includes_fixed_chat_topic_and_disables_link_previews() -> None:
    async def scenario() -> None:
        parameters: list[object] = []

        async def respond(request: web.Request) -> web.Response:
            assert request.path.endswith("/sendMessage")
            parameters.append(await request.json())
            return success({"message_id": 731})

        text = "<service> & [plain] 😃\nhttps://example.test"
        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                assert await api.send_message(-100123, text, topic_id=15) == 731
                await api.send_message(123, "status")
        assert parameters == [
            {
                "chat_id": -100123,
                "text": text,
                "message_thread_id": 15,
                "link_preview_options": {"is_disabled": True},
            },
            {"chat_id": 123, "text": "status", "link_preview_options": {"is_disabled": True}},
        ]

    asyncio.run(scenario())


@pytest.mark.parametrize("message", [None, {}, [], {"message_id": False}, {"message_id": -3}])
def test_send_requires_a_valid_accepted_message_identifier(message: object) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return success(message)

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.send_message(123, "status")
        assert caught.value.kind is TelegramErrorKind.PROTOCOL

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status", "code", "expected", "retryable"),
    [
        (400, 400, TelegramErrorKind.PERMANENT, False),
        (403, 403, TelegramErrorKind.PERMANENT, False),
        (401, 401, TelegramErrorKind.UNAUTHORIZED, False),
        (404, 404, TelegramErrorKind.UNAUTHORIZED, False),
        (409, 409, TelegramErrorKind.CONFLICT, False),
        (429, 429, TelegramErrorKind.RATE_LIMITED, True),
        (500, 500, TelegramErrorKind.TRANSIENT, True),
        (502, 502, TelegramErrorKind.TRANSIENT, True),
        (200, 401, TelegramErrorKind.UNAUTHORIZED, False),
        (200, 409, TelegramErrorKind.CONFLICT, False),
        (200, 429, TelegramErrorKind.RATE_LIMITED, True),
        (200, 500, TelegramErrorKind.TRANSIENT, True),
        (200, 499, TelegramErrorKind.PERMANENT, False),
    ],
)
def test_errors_are_categorized_without_exposing_provider_descriptions(
    status: int, code: int, expected: TelegramErrorKind, retryable: bool
) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.json_response(
                {
                    "ok": False,
                    "error_code": code,
                    "description": f"private diagnostic {TOKEN} https://private.example.test",
                },
                status=status,
            )

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.send_message(123, "status")
        assert caught.value.kind is expected
        assert caught.value.status == status
        assert caught.value.retryable is retryable
        rendered = "".join(traceback.format_exception(caught.value))
        assert TOKEN not in rendered
        assert "private diagnostic" not in rendered
        assert "private.example.test" not in rendered

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("hint", "expected"),
    [
        (15, 15),
        (0, 0),
        (True, None),
        (-1, None),
        ("15", None),
        (86401, 86401),
        (MAX_RETRY_AFTER, MAX_RETRY_AFTER),
        (MAX_RETRY_AFTER + 1, MAX_RETRY_AFTER + 1),
        (10**100, MAX_RETRY_AFTER + 1),
    ],
)
def test_retry_after_preserves_supported_hints_and_marks_overflow(
    hint: object, expected: int | None
) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.json_response(
                {"ok": False, "error_code": 429, "parameters": {"retry_after": hint}}, status=429
            )

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.send_message(123, "status")
        assert caught.value.kind is TelegramErrorKind.RATE_LIMITED
        assert caught.value.retry_after == expected

    asyncio.run(scenario())


@pytest.mark.parametrize("status", [400, 403, 429, 500, 401, 409])
def test_http_failure_categories_survive_invalid_json(status: int) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.Response(status=status, text=f"invalid body with {TOKEN}")

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.send_message(123, "status")
        assert (
            caught.value.kind
            is {
                400: TelegramErrorKind.PERMANENT,
                403: TelegramErrorKind.PERMANENT,
                429: TelegramErrorKind.RATE_LIMITED,
                500: TelegramErrorKind.TRANSIENT,
                401: TelegramErrorKind.UNAUTHORIZED,
                409: TelegramErrorKind.CONFLICT,
            }[status]
        )
        assert TOKEN not in "".join(traceback.format_exception(caught.value))

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"\xff",
        b"[]",
        b"null",
        b'{"ok":true}',
        b'{"ok":1,"result":{}}',
        b'{"ok":false,"error_code":true}',
        b'{"ok":false}',
        b"[" * 2000 + b"]" * 2000,
    ],
)
def test_malformed_envelopes_are_sanitized_protocol_errors(body: bytes) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.Response(body=body)

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.send_message(123, "status")
        assert caught.value.kind is TelegramErrorKind.PROTOCOL

    asyncio.run(scenario())


@pytest.mark.parametrize("chunked", [False, True])
def test_response_read_is_bounded_for_declared_and_chunked_body(chunked: bool) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.StreamResponse:
            body = b"x" * (MAX_RESPONSE_BYTES + 1)
            if not chunked:
                return web.Response(body=body)
            response = web.StreamResponse()
            await response.prepare(request)
            await response.write(body)
            await response.write_eof()
            return response

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.send_message(123, "status")
        assert caught.value.kind is TelegramErrorKind.PROTOCOL

    asyncio.run(scenario())


def test_compressed_response_is_not_implicitly_expanded() -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.Response(body=b"not gzip", headers={"Content-Encoding": "gzip"})

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.send_message(123, "status")
        assert caught.value.kind is TelegramErrorKind.PROTOCOL

    asyncio.run(scenario())


def test_redirect_does_not_forward_a_token_or_message() -> None:
    async def scenario() -> None:
        calls: list[str] = []

        async def respond(request: web.Request) -> web.Response:
            calls.append(request.path)
            return web.Response(status=302, headers={"Location": "/redirect-target"})

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.send_message(123, "status")
        assert caught.value.kind is TelegramErrorKind.PROTOCOL
        assert calls == [f"/bot{TOKEN}/sendMessage"]

    asyncio.run(scenario())


def test_ambient_proxy_and_provider_cookies_are_not_used(monkeypatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("NO_PROXY", "")

    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            assert "Cookie" not in request.headers
            assert "Authorization" not in request.headers
            response = success({"message_id": 731})
            response.set_cookie("provider", "ignored")
            return response

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                await api.send_message(123, "one")
                await api.send_message(123, "two")

    asyncio.run(scenario())


def test_slow_body_deadline_is_sanitized_and_the_next_request_can_succeed() -> None:
    async def scenario() -> None:
        calls = 0

        async def respond(request: web.Request) -> web.StreamResponse:
            nonlocal calls
            calls += 1
            if calls > 1:
                return success({"message_id": 731})
            response = web.StreamResponse()
            await response.prepare(request)
            await asyncio.sleep(0.1)
            return response

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url, request_timeout=0.02) as api:
                with pytest.raises(TelegramError) as caught:
                    await api.send_message(123, "one")
                assert await api.send_message(123, "two") == 731
        assert caught.value.kind is TelegramErrorKind.TRANSIENT
        assert TOKEN not in "".join(traceback.format_exception(caught.value))

    asyncio.run(scenario())


def test_long_poll_does_not_occupy_the_sender_connection_and_cancels_cleanly() -> None:
    async def scenario() -> None:
        entered = asyncio.Event()

        async def respond(request: web.Request) -> web.Response:
            if request.path.endswith("/getUpdates"):
                entered.set()
                await asyncio.Event().wait()
            return success({"message_id": 731})

        async with bot_fixture(respond) as url:
            async with TelegramAPI(TOKEN, fixture_base_url=url) as api:
                polling = asyncio.create_task(api.get_updates(None))
                await entered.wait()
                assert await asyncio.wait_for(api.send_message(123, "status"), 1) == 731
                polling.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await polling
                assert await api.send_message(123, "after cancellation") == 731

    asyncio.run(scenario())


def test_tls_failure_is_not_bypassed_or_leaked() -> None:
    async def scenario() -> None:
        async def plain_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(plain_http, "127.0.0.1", 0)
        async with server:
            port = server.sockets[0].getsockname()[1]
            async with TelegramAPI(TOKEN, fixture_base_url=f"https://127.0.0.1:{port}") as api:
                with pytest.raises(TelegramError) as caught:
                    await api.send_message(123, "status")
        assert caught.value.kind is TelegramErrorKind.TRANSIENT
        assert TOKEN not in "".join(traceback.format_exception(caught.value))

    asyncio.run(scenario())


@pytest.mark.parametrize("token", ["", "no-token", "123:abc", "0:" + "a" * 30, TOKEN + "/getMe"])
def test_invalid_tokens_fail_locally_without_echoing_the_value(token: str) -> None:
    with pytest.raises(ValueError) as caught:
        TelegramAPI(token)
    assert "invalid format" in str(caught.value)
    assert TOKEN not in str(caught.value)


@pytest.mark.parametrize(
    "origin",
    [
        "https://api.telegram.org",
        "http://localhost:8080",
        "http://192.0.2.1",
        "http://127.0.0.1/private",
        "http://user:secret@127.0.0.1",
        "http://127.0.0.1?secret=true",
        "http://127.0.0.1#fragment",
        "ftp://127.0.0.1",
        "http://127.0.0.1:0",
        "http://127.1",
    ],
)
def test_fixture_override_cannot_target_other_hosts_or_carry_credentials(origin: str) -> None:
    with pytest.raises(ValueError, match="numeric loopback origin"):
        TelegramAPI(TOKEN, fixture_base_url=origin)


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), True, 61])
def test_constructor_rejects_invalid_deadlines(timeout: float) -> None:
    with pytest.raises(ValueError):
        TelegramAPI(TOKEN, request_timeout=timeout)


@pytest.mark.parametrize(
    ("chat", "text", "topic"),
    [
        (True, "status", None),
        (0, "status", None),
        (2**52, "status", None),
        (123, "", None),
        (123, "x" * 4097, None),
        (123, "😃" * 2049, None),
        (123, "\ud800", None),
        (123, "status", True),
        (123, "status", 0),
        (123, "status", 2**31),
    ],
)
def test_invalid_send_arguments_fail_before_opening_a_session(
    chat: int, text: str, topic: int | None
) -> None:
    async def scenario() -> None:
        with pytest.raises(ValueError):
            await TelegramAPI(TOKEN).send_message(chat, text, topic_id=topic)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("offset", "timeout"), [(True, 0), (-1, 0), (2**52, 0), (None, True), (None, -1), (None, 31)]
)
def test_invalid_poll_arguments_fail_before_opening_a_session(
    offset: int | None, timeout: int
) -> None:
    async def scenario() -> None:
        with pytest.raises(ValueError):
            await TelegramAPI(TOKEN).get_updates(offset, timeout=timeout)

    asyncio.run(scenario())


def test_use_outside_context_and_double_open_remain_visible_programming_errors() -> None:
    async def scenario() -> None:
        api = TelegramAPI(TOKEN)
        with pytest.raises(RuntimeError, match="not open"):
            await api.send_message(123, "status")
        async with api:
            with pytest.raises(RuntimeError, match="already open"):
                await api.__aenter__()
        with pytest.raises(RuntimeError, match="not open"):
            await api.get_updates(None)

    asyncio.run(scenario())
