"""A synthetic Telegram inbox for the isolated offline release demo."""

import asyncio
import html
import ipaddress
import json
import signal
import time
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from aiohttp import web

from sentinel_bot.demo import (
    DEMO_TELEGRAM_CHAT_ID as CHAT_ID,
)
from sentinel_bot.demo import (
    DEMO_TELEGRAM_TOKEN as TOKEN,
)
from sentinel_bot.demo import (
    DEMO_TELEGRAM_USER_ID as USER_ID,
)

BOT_ID = 123456
BOT_USERNAME = "SentinelDemoBot"
MAX_MESSAGES = 200
MAX_UPDATES = 20
MAX_ACTIVE_REQUESTS = 10
MAX_BODY_BYTES = 32 * 1024


def _integer(value: object, *, signed: bool = False) -> bool:
    return type(value) is int and 0 < (abs(value) if signed else value) < 2**52


def _text(value: object, limit: int) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        return len(value.encode("utf-16-le")) <= limit * 2
    except UnicodeEncodeError:
        return False


async def _object(request: web.Request) -> dict[str, object]:
    if request.content_type != "application/json":
        raise web.HTTPUnsupportedMediaType
    try:
        async with asyncio.timeout(2):
            value = json.loads(await request.read())
    except TimeoutError:
        raise web.HTTPRequestTimeout from None
    except ValueError, UnicodeDecodeError:
        raise web.HTTPBadRequest from None
    if not isinstance(value, dict):
        raise web.HTTPBadRequest
    return value


def applications() -> tuple[web.Application, web.Application]:
    """Keep only bounded synthetic messages and queued command updates in memory."""
    messages: deque[dict[str, object]] = deque(maxlen=MAX_MESSAGES)
    updates: deque[dict[str, object]] = deque()
    changed = asyncio.Condition()
    mode = "up"
    accepted_count = send_attempts = failed_attempts = update_count = active = startups = 0
    polling = False

    @web.middleware
    async def bounded(
        request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
    ) -> web.StreamResponse:
        nonlocal active
        if active >= MAX_ACTIVE_REQUESTS:
            raise web.HTTPTooManyRequests(headers={"Retry-After": "1"})
        active += 1
        try:
            return await handler(request)
        finally:
            active -= 1

    def snapshot() -> dict[str, object]:
        return {
            "mode": mode,
            "startup_count": startups,
            "accepted_count": accepted_count,
            "send_attempts": send_attempts,
            "failed_attempts": failed_attempts,
            "pending_updates": len(updates),
            "messages": list(messages),
        }

    async def history(request: web.Request) -> web.Response:
        return web.json_response(snapshot(), headers={"Cache-Control": "no-store"})

    async def inbox(request: web.Request) -> web.Response:
        rows = "".join(
            f"<article><p>Message {message['message_id']} · "
            f"{html.escape(str(message['accepted_at']))}</p>"
            f"<pre>{html.escape(str(message['text']))}</pre></article>"
            for message in reversed(messages)
        )
        body = (
            '<!doctype html><html lang="en"><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta http-equiv="refresh" content="5"><title>Sentinel demo Telegram inbox</title>'
            "<body><h1>Sentinel demo Telegram inbox</h1>"
            "<p>Synthetic local messages. No real Telegram connection.</p>"
            f"<p>Delivery: {mode}; accepted: {accepted_count}; "
            f"failed attempts: {failed_attempts}. Refreshes every five seconds.</p>"
            f"{rows or '<p>No messages accepted yet.</p>'}</body></html>"
        )
        return web.Response(
            text=body,
            content_type="text/html",
            headers={
                "Cache-Control": "no-store",
                "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
                "X-Content-Type-Options": "nosniff",
            },
        )

    async def control(request: web.Request) -> web.Response:
        nonlocal mode
        _local_control(request)
        try:
            async with asyncio.timeout(2):
                selected = await request.text()
        except TimeoutError:
            raise web.HTTPRequestTimeout from None
        except UnicodeDecodeError:
            raise web.HTTPBadRequest from None
        if selected not in {"up", "down"}:
            raise web.HTTPBadRequest
        mode = selected
        return web.json_response({"mode": mode})

    async def command(request: web.Request) -> web.Response:
        nonlocal update_count
        _local_control(request)
        body = await _object(request)
        if body.keys() - {"text", "user_id", "chat_id", "message_thread_id"}:
            raise web.HTTPBadRequest
        text = body.get("text")
        user_id = body.get("user_id", USER_ID)
        chat_id = body.get("chat_id", CHAT_ID)
        topic_id = body.get("message_thread_id")
        if (
            not _text(text, 256)
            or not _integer(user_id)
            or not _integer(chat_id, signed=True)
            or ("message_thread_id" in body and not _integer(topic_id))
        ):
            raise web.HTTPBadRequest
        async with changed:
            if len(updates) >= MAX_UPDATES:
                raise web.HTTPTooManyRequests(headers={"Retry-After": "1"})
            update_count += 1
            message: dict[str, object] = {
                "message_id": update_count,
                "date": int(time.time()),
                "chat": {"id": chat_id, "type": "supergroup"},
                "from": {"id": user_id, "is_bot": False, "first_name": "Demo operator"},
                "text": text,
            }
            if topic_id is not None:
                message["message_thread_id"] = topic_id
            updates.append({"update_id": update_count, "message": message})
            changed.notify_all()
        return web.json_response({"update_id": update_count}, status=202)

    async def api(request: web.Request) -> web.Response:
        nonlocal accepted_count, send_attempts, failed_attempts, polling, startups
        if request.match_info["token"] != TOKEN:
            return web.json_response({"ok": False, "error_code": 401}, status=401)
        method = request.match_info["method"]
        if method not in {"getMe", "getWebhookInfo", "getUpdates", "sendMessage"}:
            raise web.HTTPNotFound
        body = await _object(request)
        if method == "getMe":
            startups += 1
            result: object = {"id": BOT_ID, "is_bot": True, "username": BOT_USERNAME}
        elif method == "getWebhookInfo":
            result = {"url": "", "pending_update_count": len(updates)}
        elif method == "getUpdates":
            offset = body.get("offset", 0)
            limit = body.get("limit", MAX_UPDATES)
            timeout = body.get("timeout", 0)
            if (
                body.keys() - {"offset", "limit", "timeout", "allowed_updates"}
                or type(offset) is not int
                or not 0 <= offset < 2**52
                or type(limit) is not int
                or not 1 <= limit <= MAX_UPDATES
                or type(timeout) is not int
                or not 0 <= timeout <= 30
                or body.get("allowed_updates", ["message"]) != ["message"]
            ):
                raise web.HTTPBadRequest
            if polling:
                return web.json_response({"ok": False, "error_code": 409}, status=409)
            polling = True
            try:
                async with changed:
                    while updates and int(updates[0]["update_id"]) < offset:
                        updates.popleft()
                    if not updates and timeout:
                        try:
                            async with asyncio.timeout(min(timeout, 20)):
                                while not updates:
                                    await changed.wait()
                                    while updates and int(updates[0]["update_id"]) < offset:
                                        updates.popleft()
                        except TimeoutError:
                            pass
                    result = list(updates)[:limit]
            finally:
                polling = False
        else:
            text = body.get("text")
            topic_id = body.get("message_thread_id")
            if (
                body.keys() - {"chat_id", "text", "message_thread_id", "link_preview_options"}
                or type(body.get("chat_id")) is not int
                or body["chat_id"] != CHAT_ID
                or not _text(text, 4096)
                or (
                    "message_thread_id" in body
                    and (not _integer(topic_id) or int(topic_id) >= 2**31)
                )
            ):
                raise web.HTTPBadRequest
            send_attempts += 1
            if mode == "down":
                failed_attempts += 1
                return web.json_response({"ok": False, "error_code": 503}, status=503)
            accepted_count += 1
            accepted: dict[str, object] = {
                "message_id": accepted_count,
                "chat_id": CHAT_ID,
                "text": text,
                "accepted_at": datetime.now(UTC).isoformat(),
            }
            if topic_id is not None:
                accepted["message_thread_id"] = topic_id
            messages.append(accepted)
            result = {"message_id": accepted_count}
        return web.json_response({"ok": True, "result": result})

    async def health(request: web.Request) -> web.Response:
        return web.Response(text="SYNTHETIC")

    app = web.Application(client_max_size=MAX_BODY_BYTES, middlewares=[bounded])
    app.router.add_get("/", inbox)
    app.router.add_get("/api/messages", history)
    app.router.add_get("/health", health)
    app.router.add_post("/control", control)
    app.router.add_post("/command", command)
    app.router.add_post("/bot{token}/{method}", api)
    ui = web.Application(client_max_size=MAX_BODY_BYTES, middlewares=[bounded])
    ui.router.add_get("/", inbox)
    ui.router.add_get("/api/messages", history)
    ui.router.add_get("/health", health)
    return app, ui


def _local_control(request: web.Request) -> None:
    try:
        if request.remote is not None and ipaddress.ip_address(request.remote).is_loopback:
            return
    except ValueError:
        pass
    raise web.HTTPForbidden


async def main() -> None:
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    runners: list[web.AppRunner] = []
    try:
        for port, app in zip((8080, 8081), applications(), strict=True):
            runner = web.AppRunner(
                app,
                access_log=None,
                handler_cancellation=True,
                shutdown_timeout=1,
                max_headers=32,
                max_line_size=2048,
                max_field_size=1024,
                keepalive_timeout=2,
            )
            await runner.setup()
            runners.append(runner)
            await web.TCPSite(runner, "0.0.0.0", port).start()
        await stopped.wait()
    finally:
        for runner in reversed(runners):
            await runner.cleanup()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    asyncio.run(main())
