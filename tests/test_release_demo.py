import asyncio
import importlib.util
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from sentinel_bot.config import ConfigError, Settings, SiteConfig, TelegramSettings, load_settings
from sentinel_bot.demo import (
    DEMO_SOURCE_TOKEN,
    DEMO_TELEGRAM_CHAT_ID,
    DEMO_TELEGRAM_DESTINATION,
    DEMO_TELEGRAM_TOKEN,
    DEMO_TELEGRAM_USER_ID,
)
from sentinel_bot.models import CheckResult, ErrorKind, Observation
from sentinel_bot.monitoring import MonitoringProgress
from sentinel_bot.storage import SQLiteStore, initialize_state
from sentinel_bot.telegram_api import TelegramAPI
from sentinel_bot.telegram_runtime import TelegramConfigurationError, run_telegram

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("release_demo_control", ROOT / "demo/control.py")
assert SPEC is not None and SPEC.loader is not None
control = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(control)


@pytest.mark.parametrize(
    "opening_route,closing_route",
    [
        (DEMO_TELEGRAM_DESTINATION, "telegram:-100123456:0"),
        ("telegram:-100123456:0", DEMO_TELEGRAM_DESTINATION),
    ],
)
def test_switching_demo_and_live_routes_keeps_openings_from_crossing_providers(
    tmp_path, opening_route, closing_route
):
    now = datetime.now(UTC)
    initialize_state(tmp_path, now)
    store = SQLiteStore(tmp_path)
    site = SiteConfig(
        "site", "https://example.test", failure_confirmation=1, recovery_confirmation=1
    )
    store.reconcile((site,))
    store.configure_telegram(opening_route, now)
    store.apply_observation(site, Observation(now, CheckResult(False, ErrorKind.CONNECTION)))
    assert store.pending_notifications()[0].destination == opening_route
    store.configure_telegram(closing_route, now)
    assert store.pending_notifications() == ()
    assert store.delivery_status()["failed"] == 1
    store.apply_observation(site, Observation(now + timedelta(seconds=1), CheckResult(True)))
    assert store.pending_notifications() == ()
    assert store.delivery_status()["blocked"] == 1


@pytest.mark.parametrize("route", ["telegram:demo:123:0", "telegram:demo:-100123456:1"])
def test_storage_rejects_other_synthetic_destinations(tmp_path, route):
    initialize_state(tmp_path, datetime.now(UTC))
    with pytest.raises(ValueError):
        SQLiteStore(tmp_path).configure_telegram(route, datetime.now(UTC))


def demo_environment():
    return {
        "SENTINEL_SOURCE_DEMO_TOKEN": DEMO_SOURCE_TOKEN,
        "SENTINEL_EMAIL_FROM": "sentinel@example.test",
        "SENTINEL_EMAIL_RECIPIENTS": "operator@example.test,backup@example.test",
        "SENTINEL_TELEGRAM_BOT_TOKEN": "invalid inherited credential",
        "SENTINEL_TELEGRAM_CHAT_ID": "invalid inherited destination",
        "SENTINEL_TELEGRAM_ALLOWED_USER_IDS": "invalid inherited users",
        "SENTINEL_TELEGRAM_TOPIC_ID": "invalid inherited topic",
    }


def test_complete_demo_is_synthetic_and_keeps_push_sources_out_of_probe_inventory():
    settings = load_settings(ROOT / "config/release.local.toml", demo_environment())
    assert settings.mode == "demo"
    assert settings.telegram == TelegramSettings(
        DEMO_TELEGRAM_TOKEN,
        DEMO_TELEGRAM_CHAT_ID,
        frozenset({DEMO_TELEGRAM_USER_ID}),
        provider="demo",
    )
    assert settings.email.provider == "local_smtp"
    assert settings.email.api_key == ""
    assert settings.ingress.port == 8080
    assert [(site.alias, site.method) for site in settings.sites] == [
        ("primary", "GET"),
        ("secondary", "POST"),
    ]
    assert len(settings.polled_rules) == 5
    assert [source.alias for source in settings.sources] == ["edge"]
    assert settings.sources[0].mode == "push"
    assert "edge" not in {source.alias for source in settings.polled_sources}
    assert "edge" not in {rule.alias for rule in settings.polled_rules}
    assert (
        replace(settings.telegram, provider="telegram").destination != settings.telegram.destination
    )


@pytest.mark.parametrize("mode", ["idle", "monitor"])
@pytest.mark.parametrize("enabled", [True, False])
def test_synthetic_telegram_cannot_be_enabled_in_a_live_mode(tmp_path, mode, enabled):
    content = f'[runtime]\nmode = "{mode}"\n'
    if mode == "monitor":
        content += '[[sites]]\nalias="website"\nurl="https://example.test"\n'
    content += f'[telegram]\nprovider="demo"\nenabled={str(enabled).lower()}\n'
    path = tmp_path / "invalid.toml"
    path.write_text(content)
    with pytest.raises(ConfigError, match="demo mode"):
        load_settings(path, demo_environment())


@pytest.mark.parametrize("provider", ["other", "", 1, True, []])
def test_unknown_telegram_provider_fails_locally(tmp_path, provider):
    path = tmp_path / "invalid.toml"
    path.write_text(
        '[runtime]\nmode="idle"\n[telegram]\nenabled=false\nprovider=' + json.dumps(provider)
    )
    with pytest.raises(ConfigError, match="provider"):
        load_settings(path, {})


@pytest.mark.parametrize("field", ["api_url", "origin", "token", "chat_id"])
def test_demo_telegram_has_no_operator_selected_target_or_credentials(tmp_path, field):
    path = tmp_path / "invalid.toml"
    path.write_text('[runtime]\nmode="idle"\n[telegram]\nenabled=false\n' + field + '="value"')
    with pytest.raises(ConfigError, match="Unsupported fields"):
        load_settings(path, {})


@pytest.mark.parametrize("mode,provider", [("demo", "telegram"), ("idle", "demo")])
def test_direct_runtime_settings_cannot_cross_demo_and_live_telegram(tmp_path, mode, provider):
    settings = Settings(
        tmp_path,
        mode=mode,
        telegram=TelegramSettings(
            DEMO_TELEGRAM_TOKEN,
            DEMO_TELEGRAM_CHAT_ID,
            frozenset({DEMO_TELEGRAM_USER_ID}),
            provider=provider,
        ),
    )

    async def scenario():
        with pytest.raises(TelegramConfigurationError, match="synthetic"):
            await run_telegram(settings, asyncio.Event(), MonitoringProgress(), asyncio.Lock())

    asyncio.run(scenario())
    assert not (tmp_path / "state.sqlite3").exists()


@pytest.mark.parametrize(
    "options",
    [
        {"demo": True, "fixture_base_url": "http://127.0.0.1:8080"},
        {"demo": 1},
        {"demo": "true"},
    ],
)
def test_demo_adapter_cannot_override_origin_or_coerce_its_switch(options):
    with pytest.raises(ValueError, match="fixed synthetic"):
        TelegramAPI(DEMO_TELEGRAM_TOKEN, **options)


def test_demo_adapter_refuses_a_live_credential_before_creating_a_client():
    with pytest.raises(ValueError, match="fixed synthetic"):
        TelegramAPI("987654:" + "x" * 30, demo=True)


def test_demo_event_duplicate_replays_exactly_the_saved_immutable_payload(tmp_path):
    path = tmp_path / "event.json"
    firing = control.event_payload("firing", "example", path)
    assert control.event_payload("duplicate", "ignored", path) == firing
    opening = json.loads(firing)
    resolved = control.event_payload("resolved", "example", path)
    closing = json.loads(resolved)
    assert closing["event_id"] != opening["event_id"]
    assert closing["incident_key"] == opening["incident_key"] == "example"
    assert control.event_payload("duplicate", "ignored", path) == resolved
    notification = json.loads(control.event_payload("notification", "example", path))
    assert "incident_key" not in notification


def test_demo_event_is_saved_before_a_failed_external_request(tmp_path, monkeypatch, capsys):
    path = tmp_path / "event.json"
    monkeypatch.setattr(control, "EVENT_FILE", path)

    def failed_request(*args, **kwargs):
        assert path.exists()
        raise TimeoutError

    monkeypatch.setattr(control, "request", failed_request)
    assert control.main(["event", "firing", "--key", "example"]) == 1
    original = path.read_bytes()
    assert control.main(["event", "duplicate"]) == 1
    assert path.read_bytes() == original
    assert "retry duplicate" in capsys.readouterr().err


@pytest.mark.parametrize("key", ["", "../target", "x" * 129, "bad\nkey"])
def test_demo_event_rejects_invalid_occurrence_keys_without_a_request(tmp_path, key):
    path = tmp_path / "event.json"
    with pytest.raises(ValueError, match="bounded identifier"):
        control.event_payload("firing", key, path)
    assert not path.exists()
