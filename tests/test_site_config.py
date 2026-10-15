import json
import math
from dataclasses import replace
from pathlib import Path

import pytest

from sentinel_bot.config import ConfigError, SiteConfig, load_settings


def toml_value(value: object) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return json.dumps(value, ensure_ascii=False)


def inventory(
    directory: Path,
    *,
    mode: str = "monitor",
    sites: list[dict[str, object]] | None = None,
    runtime: dict[str, object] | None = None,
) -> Path:
    if sites is None:
        sites = [{"alias": "website", "url": "https://example.test/status"}]
    lines = ["[runtime]", f"mode = {toml_value(mode)}"]
    lines.extend(f"{name} = {toml_value(value)}" for name, value in (runtime or {}).items())
    for site in sites:
        lines.append("[[sites]]")
        lines.extend(f"{name} = {toml_value(value)}" for name, value in site.items())
    path = directory / "inventory.toml"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("mode", "sites", "expected_aliases"),
    [
        ("idle", [], ()),
        (
            "idle",
            [{"alias": "disabled", "url": "https://example.test/", "enabled": False}],
            (),
        ),
        ("monitor", [{"alias": "website", "url": "https://example.test/"}], ("website",)),
        ("demo", [{"alias": "fixture", "url": "http://site-fixture:8080/"}], ("fixture",)),
    ],
)
def test_modes_require_explicit_inventory_and_only_include_enabled_sites(
    tmp_path, mode, sites, expected_aliases
):
    settings = load_settings(inventory(tmp_path, mode=mode, sites=sites), {})
    assert settings.mode == mode
    assert tuple(site.alias for site in settings.sites) == expected_aliases


@pytest.mark.parametrize(
    ("mode", "sites"),
    [
        ("idle", [{"alias": "website", "url": "https://example.test/"}]),
        ("monitor", []),
        ("demo", []),
        (
            "monitor",
            [{"alias": "disabled", "url": "https://example.test/", "enabled": False}],
        ),
        (
            "demo",
            [{"alias": "disabled", "url": "http://site-fixture:8080/", "enabled": False}],
        ),
    ],
)
def test_idle_cannot_probe_and_monitoring_cannot_silently_run_empty(tmp_path, mode, sites):
    with pytest.raises(ConfigError):
        load_settings(inventory(tmp_path, mode=mode, sites=sites), {})


def test_disabled_sites_are_filtered_after_inventory_validation(tmp_path):
    sites = [
        {"alias": "enabled", "url": "https://example.test/"},
        {"alias": "disabled", "url": "https://example.test/disabled", "enabled": False},
    ]
    settings = load_settings(inventory(tmp_path, sites=sites), {})
    assert tuple(site.alias for site in settings.sites) == ("enabled",)

    sites[1]["url"] = "http://127.0.0.1/"
    with pytest.raises(ConfigError):
        load_settings(inventory(tmp_path, sites=sites), {})


@pytest.mark.parametrize("disabled_duplicate", [False, True])
def test_disabled_entries_cannot_hide_duplicate_aliases(tmp_path, disabled_duplicate):
    sites = [
        {"alias": "website", "url": "https://example.test/"},
        {
            "alias": "website",
            "url": "https://example.test/other",
            "enabled": not disabled_duplicate,
        },
    ]
    with pytest.raises(ConfigError, match="unique"):
        load_settings(inventory(tmp_path, sites=sites), {})


def test_inventory_limit_includes_disabled_entries(tmp_path):
    sites = [
        {"alias": f"site_{index}", "url": "https://example.test/", "enabled": index == 0}
        for index in range(20)
    ]
    settings = load_settings(inventory(tmp_path, sites=sites), {})
    assert len(settings.sites) == 1
    sites.append({"alias": "site_20", "url": "https://example.test/", "enabled": False})
    with pytest.raises(ConfigError, match="at most 20"):
        load_settings(inventory(tmp_path, sites=sites), {})


@pytest.mark.parametrize(
    "content",
    [
        '[runtime]\nmode="monitor"\n[sites]\nalias="website"\nurl="https://example.test/"',
        'sites=true\n[runtime]\nmode="monitor"',
        'sites=["website"]\n[runtime]\nmode="monitor"',
        '[runtime]\nmode="monitor"\n[[sites]]',
    ],
)
def test_malformed_inventory_is_a_configuration_error(tmp_path, content):
    path = tmp_path / "inventory.toml"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(ConfigError):
        load_settings(path, {})


@pytest.mark.parametrize(
    ("url", "normalized"),
    [
        ("HTTPS://EXAMPLE.TEST.", "https://example.test/"),
        ("http://example.test", "http://example.test/"),
        ("https://example.test:443/status", "https://example.test:443/status"),
        ("http://example.test:80/status", "http://example.test:80/status"),
        ("https://bücher.example/", "https://xn--bcher-kva.example/"),
        ("https://[2606:4700:4700::1111]/", "https://[2606:4700:4700::1111]/"),
        ("https://example.test/" + "a" * (2048 - 21), "https://example.test/" + "a" * (2048 - 21)),
    ],
)
def test_urls_are_normalized_without_resolving_or_probing(tmp_path, url, normalized):
    settings = load_settings(inventory(tmp_path, sites=[{"alias": "website", "url": url}]), {})
    assert settings.sites[0].url == normalized


@pytest.mark.parametrize(
    "url",
    [
        None,
        True,
        42,
        "",
        "ftp://example.test/",
        "https:///status",
        "https://user@example.test/",
        "https://user:password@example.test/",
        "https://example.test/?q=value",
        "https://example.test/?",
        "https://example.test/#value",
        "https://example.test/#",
        "https://example.test\\status",
        "https://example.test/a b",
        "https://example.test/\nstatus",
        "https://example.test/\tstatus",
        "https://example.test/\u007fstatus",
        "https://example.test/" + "a" * 2048,
        "https://localhost/",
        "https://website.localhost/",
        "http://127.0.0.1/",
        "http://10.0.0.1/",
        "http://169.254.169.254/",
        "http://192.168.1.1/",
        "http://0.0.0.0/",
        "http://224.0.0.1/",
        "http://240.0.0.1/",
        "http://100.64.0.1/",
        "http://[::1]/",
        "http://[fc00::1]/",
        "http://[fe80::1]/",
        "http://[ff02::1]/",
        "http://[2001:db8::1]/",
        "http://127.1/",
        "http://2130706433/",
        "http://0177.0.0.1/",
        "http://0x7f000001/",
        "http://0x7f.0.0.1/",
        "https://bad_host.example/",
        "https://example.test:0/",
        "https://example.test:8080/",
        "https://example.test:65536/",
        "https://example.test:not-a-port/",
        "https://[broken/",
    ],
)
def test_unsafe_or_unsupported_urls_fail_before_workers_start(tmp_path, url):
    site = {"alias": "website"} if url is None else {"alias": "website", "url": url}
    with pytest.raises(ConfigError):
        load_settings(inventory(tmp_path, sites=[site]), {})


@pytest.mark.parametrize(
    ("mode", "url", "allowed"),
    [
        ("demo", "http://site-fixture:8080/healthy", True),
        ("demo", "http://SITE-FIXTURE.:8080/healthy", True),
        ("demo", "http://site-fixture/", False),
        ("demo", "http://site-fixture:80/", False),
        ("demo", "https://site-fixture:8080/", False),
        ("demo", "http://other-fixture:8080/", False),
        ("demo", "http://127.0.0.1:8080/", False),
        ("demo", "https://example.test/", False),
        ("monitor", "http://site-fixture:8080/", False),
        ("idle", "http://site-fixture:8080/", False),
    ],
)
def test_demo_network_exception_is_fixed_to_its_named_http_service(tmp_path, mode, url, allowed):
    path = inventory(tmp_path, mode=mode, sites=[{"alias": "fixture", "url": url}])
    if allowed:
        assert load_settings(path, {}).sites[0].url == "http://site-fixture:8080/healthy"
    else:
        with pytest.raises(ConfigError):
            load_settings(path, {})


@pytest.mark.parametrize("alias", ["", "9site", "has space", "has.dot", "a" * 33, True, 10])
def test_aliases_are_bounded_and_cannot_be_arbitrary_input(tmp_path, alias):
    with pytest.raises(ConfigError, match="alias"):
        load_settings(
            inventory(tmp_path, sites=[{"alias": alias, "url": "https://example.test/"}]), {}
        )


def test_explicit_supported_boundaries_are_accepted(tmp_path):
    site = {
        "alias": "A" + "b" * 31,
        "url": "https://example.test/",
        "marker": "é" * 2048,
        "status_min": 100,
        "status_max": 599,
        "poll_interval_seconds": 0.1,
        "connect_timeout_seconds": 0.1,
        "total_timeout_seconds": 0.2,
        "failure_confirmation": 1,
        "recovery_confirmation": 100,
        "reminder_interval_seconds": 604800,
        "max_body_bytes": 65536,
    }
    settings = load_settings(inventory(tmp_path, sites=[site], runtime={"max_concurrency": 5}), {})
    validated = settings.sites[0]
    assert validated.marker == "é" * 2048
    assert validated.failure_confirmation == 1
    assert validated.recovery_confirmation == 100
    assert validated.freshness_limit == 0.4

    site.update(
        poll_interval_seconds=86400,
        connect_timeout_seconds=30,
        total_timeout_seconds=120,
        reminder_interval_seconds=1,
        max_body_bytes=1,
        marker="x",
    )
    assert load_settings(
        inventory(tmp_path, sites=[site], runtime={"max_concurrency": 1}), {}
    ).sites


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("unknown", "synthetic-private-value"),
        ("enabled", 1),
        ("enabled", "true"),
        ("marker", ""),
        ("marker", True),
        ("marker", "a" * 4097),
        ("marker", "é" * 2049),
        ("status_min", 99),
        ("status_min", 600),
        ("status_min", True),
        ("status_max", 199),
        ("status_max", 600),
        ("status_max", 399.0),
        ("poll_interval_seconds", 0),
        ("poll_interval_seconds", 0.09),
        ("poll_interval_seconds", 86401),
        ("poll_interval_seconds", True),
        ("poll_interval_seconds", "30"),
        ("poll_interval_seconds", float("nan")),
        ("poll_interval_seconds", float("inf")),
        ("poll_interval_seconds", 10**400),
        ("connect_timeout_seconds", 0),
        ("connect_timeout_seconds", 0.09),
        ("connect_timeout_seconds", 31),
        ("total_timeout_seconds", 0.19),
        ("total_timeout_seconds", 121),
        ("total_timeout_seconds", float("-inf")),
        ("failure_confirmation", 0),
        ("failure_confirmation", 101),
        ("failure_confirmation", True),
        ("recovery_confirmation", 0),
        ("recovery_confirmation", 101),
        ("recovery_confirmation", 2.0),
        ("reminder_interval_seconds", 0),
        ("reminder_interval_seconds", 604801),
        ("max_body_bytes", 0),
        ("max_body_bytes", 65537),
        ("max_body_bytes", 1024.0),
        ("max_body_bytes", False),
    ],
)
def test_invalid_site_settings_cannot_enter_monitoring(tmp_path, field, value):
    site = {"alias": "website", "url": "https://example.test/", field: value}
    with pytest.raises(ConfigError):
        load_settings(inventory(tmp_path, sites=[site]), {})


def test_marker_limit_counts_utf8_bytes_and_respects_body_limit(tmp_path):
    site = {
        "alias": "website",
        "url": "https://example.test/",
        "max_body_bytes": 4,
        "marker": "éé",
    }
    assert load_settings(inventory(tmp_path, sites=[site]), {}).sites[0].marker == "éé"
    site["max_body_bytes"] = 3
    with pytest.raises(ConfigError, match="marker"):
        load_settings(inventory(tmp_path, sites=[site]), {})


def test_connect_timeout_cannot_exceed_whole_request_deadline(tmp_path):
    site = {
        "alias": "website",
        "url": "https://example.test/",
        "connect_timeout_seconds": 11,
        "total_timeout_seconds": 10,
    }
    with pytest.raises(ConfigError, match="total request deadline"):
        load_settings(inventory(tmp_path, sites=[site]), {})


@pytest.mark.parametrize("value", [0, 6, True, 1.0, "5"])
def test_probe_concurrency_is_a_bounded_integer(tmp_path, value):
    with pytest.raises(ConfigError, match="max_concurrency"):
        load_settings(inventory(tmp_path, runtime={"max_concurrency": value}), {})


@pytest.mark.parametrize(
    "change",
    [
        {"url": "https://example.test/new"},
        {"marker": "expected"},
        {"status_min": 201},
        {"status_max": 299},
        {"connect_timeout": 4.0},
        {"total_timeout": 9.0},
        {"failure_confirmation": 4},
        {"recovery_confirmation": 3},
        {"max_body_bytes": 4096},
    ],
)
def test_changed_check_or_confirmation_semantics_invalidate_fingerprint(change):
    original = SiteConfig("website", "https://example.test/")
    assert replace(original, **change).fingerprint != original.fingerprint


@pytest.mark.parametrize("change", [{"poll_interval": 60.0}, {"reminder_interval": 3600.0}])
def test_scheduling_changes_preserve_incident_fingerprint(change):
    original = SiteConfig("website", "https://example.test/")
    assert replace(original, **change).fingerprint == original.fingerprint


def test_normalized_url_keeps_fingerprint_stable_across_equivalent_inventory(tmp_path):
    first = load_settings(
        inventory(tmp_path, sites=[{"alias": "website", "url": "https://example.test/"}]), {}
    ).sites[0]
    second = load_settings(
        inventory(tmp_path, sites=[{"alias": "website", "url": "HTTPS://EXAMPLE.TEST."}]), {}
    ).sites[0]
    assert second.fingerprint == first.fingerprint
    assert second.freshness_limit == 70.0
    assert replace(second, poll_interval=40, total_timeout=11).freshness_limit == 91


@pytest.mark.parametrize("field", ["url", "marker", "alias", "unknown"])
def test_configuration_errors_exclude_operator_values(tmp_path, field):
    private_value = "synthetic-private-secret-do-not-print"
    value = {
        "url": f"https://user:{private_value}@example.test/",
        "marker": private_value * 200,
        "alias": private_value + " invalid",
        "unknown": private_value,
    }[field]
    site = {"alias": "website", "url": "https://example.test/", field: value}
    with pytest.raises(ConfigError) as error:
        load_settings(inventory(tmp_path, sites=[site]), {})
    assert private_value not in str(error.value)
