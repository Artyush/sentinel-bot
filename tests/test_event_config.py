import json
from pathlib import Path

import pytest

from sentinel_bot.config import ConfigError, IngressSettings, load_settings

TOKEN = "synthetic_source_credential_1234567890"


def environment() -> dict[str, str]:
    return {
        "TEST_SOURCE_TOKEN": TOKEN,
        "SENTINEL_TELEGRAM_BOT_TOKEN": "123456789:synthetic_telegram_credential_1234567890",
        "SENTINEL_TELEGRAM_CHAT_ID": "-1001234567890",
        "SENTINEL_TELEGRAM_ALLOWED_USER_IDS": "123456789",
    }


def configuration(
    tmp_path: Path,
    *,
    ingress: dict[str, object] | None = None,
    sources: list[dict[str, object]] | None = None,
    sites: list[dict[str, object]] | None = None,
    mode: str = "monitor",
    telegram: bool = True,
) -> Path:
    ingress = {"enabled": True, **(ingress or {})}
    if sources is None:
        sources = [
            {"alias": "sender", "allowed_types": ["attack"], "token_env": "TEST_SOURCE_TOKEN"}
        ]
    rows = [
        "[runtime]",
        f"mode = {json.dumps(mode)}",
        "[telegram]",
        f"enabled = {str(telegram).lower()}",
        "[ingress]",
    ]
    rows.extend(f"{key} = {value_json(value)}" for key, value in ingress.items())
    for section, values in (("sites", sites or []), ("sources", sources)):
        for row in values:
            rows.append(f"[[{section}]]")
            rows.extend(f"{key} = {value_json(value)}" for key, value in row.items())
    path = tmp_path / "ingress.toml"
    path.write_text("\n".join(rows), encoding="utf-8")
    return path


def value_json(value: object) -> str:
    if isinstance(value, float) and value != value:
        return "nan"
    return json.dumps(value, ensure_ascii=False)


def test_push_only_startup_has_no_poll_target_and_keeps_token_private(tmp_path: Path) -> None:
    settings = load_settings(configuration(tmp_path), environment())
    assert settings.ingress == IngressSettings()
    assert settings.sites == ()
    assert len(settings.sources) == 1
    source = settings.sources[0]
    assert source.alias == "sender"
    assert source.mode == "push"
    assert source.allowed_types == frozenset({"attack"})
    assert source.token == TOKEN
    assert TOKEN not in repr(settings)
    assert TOKEN not in repr(source)


def test_hybrid_requires_matching_explicit_website_and_retains_both_inputs(tmp_path: Path) -> None:
    path = configuration(
        tmp_path,
        sites=[{"alias": "sender", "url": "https://example.test/"}],
        sources=[
            {
                "alias": "sender",
                "mode": "hybrid",
                "allowed_types": ["attack", "deploy.completed"],
                "token_env": "TEST_SOURCE_TOKEN",
            }
        ],
    )
    settings = load_settings(path, environment())
    assert settings.sources[0].mode == "hybrid"
    assert settings.sites[0].alias == settings.sources[0].alias


@pytest.mark.parametrize("mode", ["push", "hybrid"])
def test_source_mode_cannot_silently_select_an_outbound_target(tmp_path: Path, mode: str) -> None:
    path = configuration(
        tmp_path,
        sites=[{"alias": "sender", "url": "https://example.test/", "enabled": False}],
        sources=[
            {
                "alias": "sender",
                "mode": mode,
                "allowed_types": ["attack"],
                "token_env": "TEST_SOURCE_TOKEN",
            }
        ],
    )
    with pytest.raises(ConfigError):
        load_settings(path, environment())


@pytest.mark.parametrize(
    "field,value",
    [
        ("url", "https://example.test/"),
        ("recipient", "operator@example.test"),
        ("mode", "poll"),
        ("mode", True),
        ("alias", "bad.alias"),
        ("alias", "a" * 33),
        ("allowed_types", []),
        ("allowed_types", "attack"),
        ("allowed_types", ["Attack"]),
        ("allowed_types", ["attack", "attack"]),
        ("allowed_types", ["a" * 65]),
        ("allowed_types", [True]),
        ("allowed_types", [f"event{i}" for i in range(21)]),
        ("token_env", "invalid-name"),
        ("token_env", "a" * 32),
        ("token_env", "A" * 129),
        ("enabled", 1),
        ("enabled", "true"),
    ],
)
def test_invalid_source_fields_rejected_before_workers(
    tmp_path: Path, field: str, value: object
) -> None:
    source = {
        "alias": "sender",
        "allowed_types": ["attack"],
        "token_env": "TEST_SOURCE_TOKEN",
        field: value,
    }
    with pytest.raises(ConfigError):
        load_settings(configuration(tmp_path, sources=[source]), environment())


@pytest.mark.parametrize(
    "token", ["", "short", "x" * 31, "x" * 129, "é" * 32, "x" * 32 + "\n", "x" * 32 + "="]
)
def test_source_credentials_are_bounded_without_error_echo(tmp_path: Path, token: str) -> None:
    env = environment()
    env["TEST_SOURCE_TOKEN"] = token
    with pytest.raises(ConfigError) as raised:
        load_settings(configuration(tmp_path), env)
    if token:
        assert token not in str(raised.value)


@pytest.mark.parametrize("size", [32, 128])
def test_token_length_boundaries(tmp_path: Path, size: int) -> None:
    env = environment()
    env["TEST_SOURCE_TOKEN"] = "x" * size
    assert load_settings(configuration(tmp_path), env).sources[0].token == "x" * size


@pytest.mark.parametrize("collision", ["alias", "token_env", "token"])
def test_each_configured_sender_has_distinct_identity_and_credential(
    tmp_path: Path, collision: str
) -> None:
    source = {"alias": "sender", "allowed_types": ["attack"], "token_env": "TEST_SOURCE_TOKEN"}
    second = {**source, "alias": "other", "token_env": "OTHER_SOURCE_TOKEN"}
    if collision != "token":
        second[collision] = source[collision]
    env = environment()
    env["OTHER_SOURCE_TOKEN"] = (
        TOKEN if collision == "token" else "another_synthetic_token_1234567890"
    )
    with pytest.raises(ConfigError):
        load_settings(configuration(tmp_path, sources=[source, second]), env)


def test_disabled_receiver_validates_inventory_without_reading_credentials(tmp_path: Path) -> None:
    path = configuration(tmp_path, ingress={"enabled": False}, mode="idle", telegram=False)
    settings = load_settings(path, {"TEST_SOURCE_TOKEN": "malformed"})
    assert settings.ingress is None
    assert settings.sources == ()


def test_disabled_sender_does_not_require_token(tmp_path: Path) -> None:
    path = configuration(
        tmp_path,
        sources=[
            {"alias": "sender", "allowed_types": ["attack"], "token_env": "TEST_SOURCE_TOKEN"},
            {
                "alias": "off",
                "allowed_types": ["attack"],
                "token_env": "OFF_TOKEN",
                "enabled": False,
            },
        ],
    )
    assert len(load_settings(path, environment()).sources) == 1


@pytest.mark.parametrize("change", ["idle", "no_sources", "disabled_sources", "no_channel"])
def test_receiver_requires_enabled_source_route_and_monitor_mode(
    tmp_path: Path, change: str
) -> None:
    kwargs: dict[str, object] = {}
    if change == "idle":
        kwargs["mode"] = "idle"
    elif change == "no_sources":
        kwargs["sources"] = []
    elif change == "disabled_sources":
        kwargs["sources"] = [
            {
                "alias": "off",
                "allowed_types": ["attack"],
                "token_env": "OFF_TOKEN",
                "enabled": False,
            }
        ]
    else:
        kwargs["telegram"] = False
    with pytest.raises(ConfigError):
        load_settings(configuration(tmp_path, **kwargs), environment())


@pytest.mark.parametrize(
    "field,value",
    [
        ("enabled", "true"),
        ("enabled", 1),
        ("unknown", True),
        ("host", "example.test"),
        ("host", "192.0.2.1"),
        ("host", True),
        ("port", 80),
        ("port", 65536),
        ("port", True),
        ("max_body_bytes", 0),
        ("max_body_bytes", 16385),
        ("max_summary_chars", 1001),
        ("body_timeout_seconds", 0),
        ("body_timeout_seconds", 31),
        ("max_concurrency", 11),
        ("source_rate_per_minute", 0),
        ("global_burst", 0),
        ("max_event_age_seconds", 0),
        ("future_skew_seconds", -1),
        ("future_skew_seconds", float("nan")),
        ("retention_seconds", 172799),
        ("max_receipts", 10001),
        ("max_tombstones_per_source", 1001),
        ("max_active_incidents", 0),
        ("max_pending", 1001),
    ],
)
def test_ingress_limits_are_bounded(tmp_path: Path, field: str, value: object) -> None:
    with pytest.raises(ConfigError):
        load_settings(configuration(tmp_path, ingress={field: value}), environment())


@pytest.mark.parametrize(
    "limits",
    [
        {"max_event_age_seconds": 172800, "future_skew_seconds": 1},
        {"max_receipts": 999},
        {"max_tombstones": 999},
        {"max_pending": 199},
        {"max_active_incidents": 49},
        {"global_rate_per_minute": 59},
        {"global_burst": 9},
        {"max_pending": 399},
        {"max_pending_per_source": 99},
    ],
)
def test_retention_and_reserved_lifecycle_capacity_must_be_consistent(
    tmp_path: Path, limits: dict[str, object]
) -> None:
    with pytest.raises(ConfigError):
        load_settings(configuration(tmp_path, ingress=limits), environment())


def test_global_inventory_limit_counts_disabled_sources_and_sites(tmp_path: Path) -> None:
    sources = [
        {
            "alias": f"sender{i}",
            "allowed_types": ["attack"],
            "token_env": f"TOKEN_{i}",
            "enabled": False,
        }
        for i in range(20)
    ]
    path = configuration(
        tmp_path,
        ingress={"enabled": False},
        sources=sources,
        sites=[{"alias": "website", "url": "https://example.test/"}],
    )
    with pytest.raises(ConfigError, match="20 distinct"):
        load_settings(path, environment())


def test_twenty_hybrid_entries_count_as_twenty_distinct_sources(tmp_path: Path) -> None:
    env = environment()
    sources = []
    sites = []
    for index in range(20):
        alias = f"sender{index}"
        token_env = f"TOKEN_{index}"
        env[token_env] = f"synthetic_unique_credential_{index:06d}"
        sources.append(
            {"alias": alias, "mode": "hybrid", "allowed_types": ["attack"], "token_env": token_env}
        )
        sites.append({"alias": alias, "url": "https://example.test/"})
    settings = load_settings(configuration(tmp_path, sites=sites, sources=sources), env)
    assert len(settings.sources) == len(settings.sites) == 20


def test_push_capacity_reservation_does_not_consume_separate_website_budget(tmp_path: Path) -> None:
    settings = load_settings(
        configuration(
            tmp_path,
            ingress={"max_pending": 400},
            sites=[{"alias": "website", "url": "https://example.test/"}],
        ),
        environment(),
    )
    assert settings.ingress is not None
    assert settings.ingress.max_pending == 2 * settings.ingress.max_active_incidents
    assert len(settings.sites) == len(settings.sources) == 1
