import asyncio
import time
from collections import defaultdict
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from aiohttp import web

from sentinel_bot.config import load_settings
from sentinel_bot.http import HttpChecker
from sentinel_bot.models import CheckResult, ErrorKind, EventKind, Observation, SiteState
from sentinel_bot.monitoring import MonitoringProgress, monitor_sites
from sentinel_bot.storage import SQLiteStore, initialize_state


async def wait_until(condition, timeout=4):
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.005)


def test_external_inventory_runs_independent_get_and_post_checks(tmp_path, monkeypatch):
    import sentinel_bot.monitoring as monitoring

    inventory = tmp_path / "checks.toml"
    inventory.write_text("""[[sites]]
alias = "homepage"
url = "https://example.test/homepage"
poll_interval_seconds = 0.1
marker = "READY"

[[sites]]
alias = "api_probe"
url = "https://example.test/probe"
method = "POST"
json_body = { probe = "availability", sample = "Привет" }
poll_interval_seconds = 1
total_timeout_seconds = 2
connect_timeout_seconds = 1
failure_confirmation = 1
recovery_confirmation = 1
marker = "READY"

[[sites]]
alias = "disabled"
url = "https://example.test/disabled"
method = "POST"
enabled = false
""")
    main = tmp_path / "application.toml"
    main.write_text('[runtime]\nmode = "monitor"\nsites_file = "checks.toml"\n')
    state = tmp_path / "state"
    state.mkdir()
    settings = load_settings(main, {"SENTINEL_STATE_DIR": str(state)})
    initialize_state(state, datetime.now(UTC))
    store = SQLiteStore(state)
    store.reconcile(settings.sites)

    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        healthy = False
        calls = defaultdict(list)
        active = defaultdict(int)
        maximum = defaultdict(int)
        payloads = []

        async def handle(request):
            path = request.path
            calls[path].append(time.monotonic())
            active[path] += 1
            maximum[path] = max(maximum[path], active[path])
            try:
                if path == "/homepage":
                    assert request.method == "GET"
                    return web.Response(text="READY")
                assert path == "/probe"
                assert request.method == "POST"
                assert request.content_type == "application/json"
                payloads.append(await request.json())
                if len(calls[path]) == 1:
                    entered.set()
                    await release.wait()
                return web.Response(status=200 if healthy else 503, text="READY")
            finally:
                active[path] -= 1

        app = web.Application()
        app.router.add_route("*", "/{path:.*}", handle)
        runner = web.AppRunner(app, shutdown_timeout=0.1)
        await runner.setup()
        server = web.TCPSite(runner, "127.0.0.1", 0)
        await server.start()
        origin = f"http://127.0.0.1:{runner.addresses[0][1]}"

        class FixtureChecker:
            def __init__(self, max_concurrency, fixture_hosts, *, semaphore=None):
                self.inner = HttpChecker(
                    max_concurrency, frozenset({"127.0.0.1"}), semaphore=semaphore
                )

            async def __aenter__(self):
                await self.inner.__aenter__()
                return self

            async def __aexit__(self, *args):
                await self.inner.__aexit__(*args)

            async def check(self, site):
                path = "/homepage" if site.alias == "homepage" else "/probe"
                return await self.inner.check(replace(site, url=origin + path))

        monkeypatch.setattr(monitoring, "HttpChecker", FixtureChecker)
        stopped = asyncio.Event()
        progress = MonitoringProgress()
        task = asyncio.create_task(monitor_sites(settings, stopped, progress))
        try:
            await asyncio.wait_for(entered.wait(), 3)
            await wait_until(lambda: len(calls["/homepage"]) >= 3)
            assert len(calls["/probe"]) == 1
            assert store.read_site("homepage").state.confirmed is SiteState.UP
            assert store.read_site("api_probe").observation is None

            release.set()
            await wait_until(lambda: store.read_site("api_probe").state.confirmed is SiteState.DOWN)
            opening = store.pending_notifications()[0]
            assert opening.source_alias == "api_probe"
            assert opening.event.kind is EventKind.DOWN
            healthy = True
            await wait_until(lambda: store.read_site("api_probe").state.confirmed is SiteState.UP)
            events = store.pending_notifications()
            assert [item.event.kind for item in events] == [EventKind.DOWN, EventKind.RECOVERY]
            assert all(item.event.incident_id == opening.event.incident_id for item in events)
            assert all(item.source_alias == "api_probe" for item in events)
            assert calls["/probe"][1] - calls["/probe"][0] >= 0.75
            assert maximum["/homepage"] == maximum["/probe"] == 1
            assert len(payloads) >= 2
            assert all(
                payload == {"probe": "availability", "sample": "Привет"} for payload in payloads
            )
            assert set(progress.observations) == {"homepage", "api_probe"}
            assert [item.alias for item in store.read_sites()] == ["api_probe", "homepage"]
        finally:
            release.set()
            stopped.set()
            await asyncio.wait_for(task, 3)
            await runner.cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("change", ["move", "method", "body"])
def test_external_inventory_reconciliation_preserves_unaffected_incidents(tmp_path, change):
    extra = 'method = "POST"\njson_body = { probe = "first" }\n' if change == "body" else ""
    entries = f"""[[sites]]
alias = "first"
url = "https://example.test/first"
{extra}
[[sites]]
alias = "second"
url = "https://example.test/second"
"""
    main = tmp_path / "application.toml"
    main.write_text('[runtime]\nmode = "monitor"\n' + entries)
    initial = load_settings(main, {"SENTINEL_STATE_DIR": str(tmp_path)})
    initialize_state(tmp_path, datetime.now(UTC))
    store = SQLiteStore(tmp_path)
    store.reconcile(initial.sites)
    now = datetime.now(UTC)
    for site in initial.sites:
        for index in range(3):
            store.apply_observation(
                site,
                Observation(now + timedelta(seconds=index), CheckResult(False, ErrorKind.TIMEOUT)),
            )
    identities = {
        site.alias: store.read_site(site.alias).state.incident_id for site in initial.sites
    }
    queued = store.pending_notifications()

    if change == "method":
        entries = entries.replace('alias = "first"', 'alias = "first"\nmethod = "POST"')
    elif change == "body":
        entries = entries.replace('probe = "first"', 'probe = "changed"')
    (tmp_path / "checks.toml").write_text(entries)
    main.write_text('[runtime]\nmode = "monitor"\nsites_file = "checks.toml"\n')
    updated = load_settings(main, {"SENTINEL_STATE_DIR": str(tmp_path)})
    store.reconcile(updated.sites)

    assert store.read_site("second").state.incident_id == identities["second"]
    if change == "move":
        assert store.read_site("first").state.incident_id == identities["first"]
        assert store.pending_notifications() == queued
    else:
        assert store.read_site("first").state.confirmed is SiteState.UNKNOWN
        assert store.read_site("first").state.incident_id is None
        assert [item.source_alias for item in store.pending_notifications()] == ["second"]
