import asyncio
import email.policy
import traceback
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from email.parser import BytesParser
from email.utils import format_datetime

import aiohttp
import pytest
from aiohttp import web

from sentinel_bot.email_api import (
    MAX_RESPONSE_BYTES,
    MAX_RETRY_AFTER,
    EmailError,
    LocalSMTPEmailAPI,
    MailchimpEmailAPI,
)

KEY = "synthetic_credential_123456789"
SENDER = "sentinel@example.test"
RECIPIENT = "operator@example.test"


@asynccontextmanager
async def provider_fixture(
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> AsyncIterator[str]:
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handler)
    runner = web.AppRunner(app, shutdown_timeout=0.1)
    await runner.setup()
    server = web.TCPSite(runner, "127.0.0.1", 0)
    await server.start()
    try:
        yield f"http://127.0.0.1:{runner.addresses[0][1]}"
    finally:
        await runner.cleanup()


def accepted(status: str = "sent") -> list[dict[str, str]]:
    return [{"email": RECIPIENT, "status": status, "_id": "synthetic-message-id"}]


@pytest.mark.parametrize("status", ["sent", "queued", "scheduled"])
def test_mailchimp_submits_one_fixed_recipient_and_checks_provider_acceptance(status: str) -> None:
    async def scenario() -> None:
        calls = []

        async def respond(request: web.Request) -> web.Response:
            calls.append((request.method, request.path, await request.json()))
            return web.json_response(accepted(status))

        async with provider_fixture(respond) as url:
            async with MailchimpEmailAPI(KEY, SENDER, fixture_base_url=url) as api:
                await api.send(RECIPIENT, "[Sentinel] DOWN: example", "Unicode summary: Диск 🔔\n")
        assert calls == [
            (
                "POST",
                "/api/1.4/messages/send",
                {
                    "key": KEY,
                    "message": {
                        "from_email": SENDER,
                        "to": [{"email": RECIPIENT, "type": "to"}],
                        "subject": "[Sentinel] DOWN: example",
                        "text": "Unicode summary: Диск 🔔\n",
                    },
                    "async": False,
                },
            )
        ]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status", "category", "retryable", "channel_wide"),
    [
        (401, "unauthorized", False, True),
        (403, "unauthorized", False, True),
        (402, "quota", True, True),
        (429, "rate_limit", True, True),
        (500, "server_error", True, False),
        (503, "server_error", True, False),
        (400, "configuration", False, True),
        (404, "configuration", False, True),
    ],
)
def test_http_failure_classification_discards_provider_details(
    status: int,
    category: str,
    retryable: bool,
    channel_wide: bool,
) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.Response(status=status, text=f"private {KEY} {RECIPIENT}")

        async with provider_fixture(respond) as url:
            async with MailchimpEmailAPI(KEY, SENDER, fixture_base_url=url) as api:
                with pytest.raises(EmailError) as caught:
                    await api.send(RECIPIENT, "Subject", "Body")
        assert caught.value.category == category and caught.value.retryable is retryable
        assert caught.value.channel_wide is channel_wide
        if status == 402:
            assert caught.value.retry_after == 3600
        rendered = "".join(traceback.format_exception(caught.value))
        assert KEY not in rendered and RECIPIENT not in rendered and "private" not in rendered

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("response", "category", "retryable", "channel_wide"),
    [
        (
            [{"email": RECIPIENT, "status": "rejected", "reject_reason": "hard-bounce"}],
            "rejected",
            False,
            False,
        ),
        (
            [{"email": RECIPIENT, "status": "rejected", "reject_reason": "unsigned"}],
            "configuration",
            False,
            True,
        ),
        ([{"email": RECIPIENT, "status": "invalid"}], "invalid_recipient", False, False),
        ({"status": "error", "name": "Invalid_Key", "message": KEY}, "unauthorized", False, True),
        ({"status": "error", "name": "PaymentRequired"}, "quota", True, True),
        ({"status": "error", "name": "ValidationError"}, "configuration", False, True),
        ({"status": "error", "name": "UnknownError"}, "invalid_response", True, False),
        ([], "invalid_response", True, False),
        ({}, "invalid_response", True, False),
        (None, "invalid_response", True, False),
        (accepted() * 2, "invalid_response", True, False),
        ([None], "invalid_response", True, False),
        (
            [{"email": "other@example.test", "status": "sent", "_id": "id"}],
            "invalid_response",
            True,
            False,
        ),
        ([{"email": RECIPIENT, "status": "sent"}], "invalid_response", True, False),
        ([{"email": RECIPIENT, "status": "sent", "_id": ""}], "invalid_response", True, False),
        (
            [{"email": RECIPIENT, "status": "sent", "_id": "x" * 257}],
            "invalid_response",
            True,
            False,
        ),
        ([{"email": RECIPIENT, "status": [], "_id": "id"}], "invalid_response", True, False),
    ],
)
def test_http_200_without_one_valid_recipient_acceptance_is_not_success(
    response: object,
    category: str,
    retryable: bool,
    channel_wide: bool,
) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.json_response(response)

        async with provider_fixture(respond) as url:
            async with MailchimpEmailAPI(KEY, SENDER, fixture_base_url=url) as api:
                with pytest.raises(EmailError) as caught:
                    await api.send(RECIPIENT, "Subject", "Body")
        assert caught.value.category == category
        assert caught.value.retryable is retryable and caught.value.channel_wide is channel_wide

    asyncio.run(scenario())


@pytest.mark.parametrize("code", [301, 302, 303, 307, 308])
def test_post_redirects_never_replay_credentials_or_message(code: int) -> None:
    async def scenario() -> None:
        calls = []

        async def respond(request: web.Request) -> web.Response:
            calls.append(request.path)
            return web.Response(status=code, headers={"Location": "/redirected"})

        async with provider_fixture(respond) as url:
            async with MailchimpEmailAPI(KEY, SENDER, fixture_base_url=url) as api:
                with pytest.raises(EmailError, match="configuration"):
                    await api.send(RECIPIENT, "Subject", "Body")
        assert calls == ["/api/1.4/messages/send"]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "retry_after",
    ["60", str(MAX_RETRY_AFTER), str(MAX_RETRY_AFTER + 1), "9999999999999999", "nonsense"],
)
def test_retry_hints_are_bounded_without_retrying_earlier_than_provider(retry_after: str) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.Response(status=429, headers={"Retry-After": retry_after})

        async with provider_fixture(respond) as url:
            async with MailchimpEmailAPI(KEY, SENDER, fixture_base_url=url) as api:
                with pytest.raises(EmailError) as caught:
                    await api.send(RECIPIENT, "Subject", "Body")
        error = caught.value
        if retry_after in {str(MAX_RETRY_AFTER + 1), "9999999999999999"}:
            assert error.category == "configuration" and not error.retryable
        else:
            assert error.category == "rate_limit"
            assert error.retry_after == (None if retry_after == "nonsense" else float(retry_after))

    asyncio.run(scenario())


def test_http_date_retry_hint_is_interpreted_as_bounded_delay() -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.Response(
                status=429,
                headers={
                    "Retry-After": format_datetime(
                        datetime.now(UTC) + timedelta(seconds=60), usegmt=True
                    )
                },
            )

        async with provider_fixture(respond) as url:
            async with MailchimpEmailAPI(KEY, SENDER, fixture_base_url=url) as api:
                with pytest.raises(EmailError) as caught:
                    await api.send(RECIPIENT, "Subject", "Body")
        assert caught.value.retry_after is not None and 58 < caught.value.retry_after <= 60

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "response_kind", ["malformed", "declared-large", "chunked-large", "compressed"]
)
def test_response_protocol_and_body_are_bounded(response_kind: str) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.StreamResponse:
            if response_kind == "malformed":
                return web.Response(body=b"not-json")
            if response_kind == "compressed":
                return web.Response(body=b"body", headers={"Content-Encoding": "gzip"})
            if response_kind == "declared-large":
                return web.Response(body=b"x" * (MAX_RESPONSE_BYTES + 1))
            response = web.StreamResponse()
            await response.prepare(request)
            try:
                await response.write(b"x" * (MAX_RESPONSE_BYTES + 1))
                await response.write_eof()
            except ConnectionResetError:
                pass
            return response

        async with provider_fixture(respond) as url:
            async with MailchimpEmailAPI(KEY, SENDER, fixture_base_url=url) as api:
                with pytest.raises(EmailError, match="invalid_response"):
                    await api.send(RECIPIENT, "Subject", "Body")

    asyncio.run(scenario())


def test_http_total_deadline_includes_body_and_does_not_replay() -> None:
    async def scenario() -> None:
        calls = []
        gate = asyncio.Event()

        async def respond(request: web.Request) -> web.StreamResponse:
            calls.append(await request.json())
            response = web.StreamResponse()
            await response.prepare(request)
            await gate.wait()
            return response

        async with provider_fixture(respond) as url:
            async with MailchimpEmailAPI(
                KEY, SENDER, fixture_base_url=url, request_timeout=0.1
            ) as api:
                try:
                    with pytest.raises(EmailError, match="timeout"):
                        await api.send(RECIPIENT, "Subject", "Body")
                finally:
                    gate.set()
        assert len(calls) == 1

    asyncio.run(scenario())


def test_lost_http_response_after_received_body_is_not_replayed() -> None:
    async def scenario() -> None:
        calls = []

        async def respond(request: web.Request) -> web.Response:
            calls.append(await request.json())
            assert request.transport is not None
            request.transport.close()
            return web.Response()

        async with provider_fixture(respond) as url:
            async with MailchimpEmailAPI(KEY, SENDER, fixture_base_url=url) as api:
                with pytest.raises(EmailError, match="network"):
                    await api.send(RECIPIENT, "Subject", "Body")
        assert len(calls) == 1

    asyncio.run(scenario())


def test_mailchimp_ignores_ambient_proxies_and_provider_cookies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for variable in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "ALL_PROXY"):
        monkeypatch.setenv(variable, "http://127.0.0.1:1")
    monkeypatch.setenv("NO_PROXY", "")

    async def scenario() -> None:
        calls = []

        async def respond(request: web.Request) -> web.Response:
            assert "Cookie" not in request.headers and "Authorization" not in request.headers
            assert request.headers["Accept-Encoding"] == "identity"
            calls.append(await request.json())
            response = web.json_response(accepted())
            response.set_cookie("provider", "private-cookie-must-not-be-replayed")
            return response

        async with provider_fixture(respond) as url:
            async with MailchimpEmailAPI(KEY, SENDER, fixture_base_url=url) as api:
                await api.send(RECIPIENT, "Subject", "First")
                await api.send(RECIPIENT, "Subject", "Second")
        assert len(calls) == 2

    asyncio.run(scenario())


def test_http_cancellation_propagates_and_context_closes_connections() -> None:
    async def scenario() -> None:
        received = asyncio.Event()
        release = asyncio.Event()

        async def respond(request: web.Request) -> web.Response:
            await request.json()
            received.set()
            await release.wait()
            return web.json_response(accepted())

        async with provider_fixture(respond) as url:
            async with MailchimpEmailAPI(KEY, SENDER, fixture_base_url=url) as api:
                task = asyncio.create_task(api.send(RECIPIENT, "Subject", "Body"))
                await received.wait()
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                release.set()
            assert api._session is None and api._resolver is None

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "value",
    [
        "https://example.com",
        "http://localhost:1234",
        "http://127.0.0.1/a",
        "http://127.0.0.1/?key=secret",
        "http://user@127.0.0.1",
        "file:///tmp/socket",
    ],
)
def test_http_fixture_seam_cannot_select_a_remote_or_credentialed_target(value: str) -> None:
    with pytest.raises(ValueError, match="loopback"):
        MailchimpEmailAPI(KEY, SENDER, fixture_base_url=value)


@pytest.mark.parametrize(
    ("subject", "body"),
    [
        ("Injected\r\nBcc: victim@example.test", "Body"),
        ("\x00subject", "Body"),
        ("x" * 201, "Body"),
        ("Subject", "x" * 16385),
        ("Subject", "body\r\nInjected"),
        ("Subject", "\ud800"),
        ("", "Body"),
        ("Subject", ""),
    ],
)
def test_message_headers_and_text_limits_are_validated_before_network(
    subject: str, body: str
) -> None:
    async def scenario() -> None:
        api = MailchimpEmailAPI(KEY, SENDER)
        with pytest.raises(ValueError):
            await api.send(RECIPIENT, subject, body)
        local = LocalSMTPEmailAPI(SENDER)
        with pytest.raises(ValueError):
            await local.send(RECIPIENT, subject, body)

    asyncio.run(scenario())


@asynccontextmanager
async def smtp_fixture(
    handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]],
) -> AsyncIterator[int]:
    active: set[asyncio.Task[None]] = set()

    async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        active.add(task)
        try:
            await handler(reader, writer)
        finally:
            writer.close()
            await writer.wait_closed()
            active.remove(task)

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        await server.wait_closed()
        if active:
            async with asyncio.timeout(2):
                await asyncio.gather(*active)


def smtp_handler(messages: list[bytes], *, recipient_code: int = 250, final_code: int = 250):
    async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(b"220 synthetic fixture\r\n")
        await writer.drain()
        while line := await reader.readline():
            if line.startswith(b"EHLO"):
                writer.write(b"250-localhost\r\n250 PIPELINING\r\n")
            elif line.startswith(b"RCPT"):
                writer.write(f"{recipient_code} synthetic response\r\n".encode())
            elif line == b"DATA\r\n":
                writer.write(b"354 send message\r\n")
                await writer.drain()
                lines = []
                while (data := await reader.readline()) != b".\r\n":
                    if not data:
                        return
                    lines.append(data[1:] if data.startswith(b"..") else data)
                messages.append(b"".join(lines))
                writer.write(f"{final_code} synthetic response\r\n".encode())
            else:
                writer.write(b"250 ok\r\n")
            await writer.drain()

    return respond


def test_local_smtp_submits_safe_plaintext_mime_and_dot_stuffs_body() -> None:
    async def scenario() -> None:
        messages: list[bytes] = []
        async with smtp_fixture(smtp_handler(messages)) as port:
            async with LocalSMTPEmailAPI(SENDER, host="127.0.0.1", port=port) as api:
                await api.send(
                    RECIPIENT, "[Sentinel] DOWN: example", "Unicode Диск 🔔\n.dot\n..two\n"
                )
        assert len(messages) == 1
        message = BytesParser(policy=email.policy.default).parsebytes(messages[0])
        assert message["From"] == SENDER and message["To"] == RECIPIENT
        assert message["Subject"] == "[Sentinel] DOWN: example"
        assert message.get_content_type() == "text/plain"
        assert message.get_content().replace("\r\n", "\n") == "Unicode Диск 🔔\n.dot\n..two\n"

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("recipient_code", "final_code", "category", "retryable"),
    [
        (550, 250, "invalid_recipient", False),
        (450, 250, "server_error", True),
        (250, 554, "configuration", False),
        (250, 421, "server_error", True),
    ],
)
def test_local_smtp_requires_final_data_acceptance_and_classifies_rejection(
    recipient_code: int, final_code: int, category: str, retryable: bool
) -> None:
    async def scenario() -> None:
        messages: list[bytes] = []
        async with smtp_fixture(
            smtp_handler(messages, recipient_code=recipient_code, final_code=final_code)
        ) as port:
            async with LocalSMTPEmailAPI(SENDER, host="127.0.0.1", port=port) as api:
                with pytest.raises(EmailError) as caught:
                    await api.send(RECIPIENT, "Subject", "Body")
        assert caught.value.category == category and caught.value.retryable is retryable
        assert len(messages) == (1 if recipient_code == 250 else 0)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "reply",
    [
        b"220 " + b"x" * 513 + b"\r\n",
        b"220-multi\r\n" * 33,
        b"220-multi\r\n221 mismatch\r\n",
        b"not SMTP\r\n",
        b"220 no CRLF\n",
        b"999 invalid\r\n",
    ],
)
def test_local_smtp_response_is_bounded_and_strict(reply: bytes) -> None:
    async def scenario() -> None:
        async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.write(reply)
            await writer.drain()
            await reader.read()

        async with smtp_fixture(respond) as port:
            async with LocalSMTPEmailAPI(SENDER, host="127.0.0.1", port=port) as api:
                with pytest.raises(EmailError, match="invalid_response"):
                    await api.send(RECIPIENT, "Subject", "Body")

    asyncio.run(scenario())


@pytest.mark.parametrize("cancel", [False, True])
def test_local_smtp_timeout_and_cancellation_close_socket_without_blocking_loop(
    cancel: bool,
) -> None:
    async def scenario() -> None:
        received = asyncio.Event()
        closed = asyncio.Event()

        async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            received.set()
            assert await reader.read() == b""
            closed.set()

        async with smtp_fixture(respond) as port:
            async with LocalSMTPEmailAPI(
                SENDER, host="127.0.0.1", port=port, request_timeout=0.1
            ) as api:
                task = asyncio.create_task(api.send(RECIPIENT, "Subject", "Body"))
                await received.wait()
                if cancel:
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                else:
                    with pytest.raises(EmailError, match="timeout"):
                        await task
                async with asyncio.timeout(1):
                    await closed.wait()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("sender", "host", "port"),
    [
        ("sender@example.com", "mailpit", 1025),
        (SENDER, "remote.example.test", 1025),
        (SENDER, "mailpit", 25),
        (SENDER, "127.0.0.1", 0),
        (SENDER, "127.0.0.1", True),
    ],
)
def test_local_smtp_refuses_real_addresses_and_remote_relays(
    sender: str, host: str, port: int
) -> None:
    with pytest.raises(ValueError):
        LocalSMTPEmailAPI(sender, host=host, port=port)


def test_local_smtp_refuses_real_recipient_before_connecting() -> None:
    async def scenario() -> None:
        async with LocalSMTPEmailAPI(SENDER) as api:
            with pytest.raises(ValueError, match="synthetic"):
                await api.send("operator@example.com", "Subject", "Body")

    asyncio.run(scenario())


def test_transports_require_context_and_reject_double_open() -> None:
    async def scenario() -> None:
        for api in (MailchimpEmailAPI(KEY, SENDER), LocalSMTPEmailAPI(SENDER)):
            with pytest.raises(RuntimeError, match="not open"):
                await api.send(RECIPIENT, "Subject", "Body")
            async with api:
                with pytest.raises(RuntimeError, match="already open"):
                    await api.__aenter__()

    asyncio.run(scenario())


def test_tls_failures_are_sanitized_without_disabling_certificate_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        def failed(*args: object, **kwargs: object):
            assert kwargs["allow_redirects"] is False
            raise aiohttp.ClientSSLError(None, OSError(f"private {KEY} {RECIPIENT}"))

        async with MailchimpEmailAPI(KEY, SENDER) as api:
            assert api._session is not None
            monkeypatch.setattr(api._session, "post", failed)
            with pytest.raises(EmailError) as caught:
                await api.send(RECIPIENT, "Subject", "Body")
            assert caught.value.category == "tls" and caught.value.retryable
            assert caught.value.__suppress_context__
            assert KEY not in "".join(traceback.format_exception(caught.value))

    asyncio.run(scenario())


@pytest.mark.parametrize("provider", ["mailchimp", "local_smtp"])
def test_repeated_cancellation_finishes_owned_context_cleanup(
    monkeypatch: pytest.MonkeyPatch, provider: str
) -> None:
    async def scenario() -> None:
        entered = asyncio.Event()
        closing = asyncio.Event()
        release = asyncio.Event()
        api = (
            MailchimpEmailAPI(KEY, SENDER)
            if provider == "mailchimp"
            else LocalSMTPEmailAPI(SENDER, host="localhost")
        )

        async def worker() -> None:
            async with api:
                if isinstance(api, MailchimpEmailAPI):
                    resource = api._session
                else:
                    resource = api._resolver
                assert resource is not None
                close = resource.close

                async def gated_close() -> None:
                    closing.set()
                    await release.wait()
                    await close()

                monkeypatch.setattr(resource, "close", gated_close)
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(worker())
        await entered.wait()
        task.cancel()
        await closing.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert api._resolver is None
        if isinstance(api, MailchimpEmailAPI):
            assert api._session is None and api._connector is None
        else:
            assert not api._open

    asyncio.run(scenario())


def test_partial_mailchimp_initialization_closes_connector_and_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        connectors = []

        def refused(*args: object, **kwargs: object) -> None:
            connectors.append(kwargs["connector"])
            raise RuntimeError("synthetic session initialization failure")

        monkeypatch.setattr("sentinel_bot.email_api.aiohttp.ClientSession", refused)
        api = MailchimpEmailAPI(KEY, SENDER)
        with pytest.raises(RuntimeError, match="initialization failure"):
            await api.__aenter__()
        assert len(connectors) == 1 and connectors[0].closed
        assert api._session is None and api._connector is None and api._resolver is None

    asyncio.run(scenario())


def test_repeated_smtp_send_cancellation_waits_for_socket_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        received = asyncio.Event()
        closing = asyncio.Event()
        release = asyncio.Event()

        async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            received.set()
            assert await reader.read() == b""

        async with smtp_fixture(respond) as port:
            close = asyncio.StreamWriter.wait_closed

            async def gated_close(writer: asyncio.StreamWriter) -> None:
                if writer.get_extra_info("peername")[1] == port:
                    closing.set()
                    await release.wait()
                await close(writer)

            monkeypatch.setattr(asyncio.StreamWriter, "wait_closed", gated_close)
            async with LocalSMTPEmailAPI(SENDER, host="127.0.0.1", port=port) as api:
                task = asyncio.create_task(api.send(RECIPIENT, "Subject", "Body"))
                await received.wait()
                task.cancel()
                await closing.wait()
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done()
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await task

    asyncio.run(scenario())


def test_local_smtp_hostname_resolution_uses_async_resolver_not_default_threadpool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        messages: list[bytes] = []
        async with smtp_fixture(smtp_handler(messages)) as port:
            async with LocalSMTPEmailAPI(SENDER, host="localhost", port=port) as api:

                async def forbidden(*args: object, **kwargs: object) -> None:
                    pytest.fail("local email must not create blocking resolver work")

                monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", forbidden)
                await api.send(RECIPIENT, "Subject", "Body")
        assert len(messages) == 1

    asyncio.run(scenario())
