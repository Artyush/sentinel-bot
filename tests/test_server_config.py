import json
from collections.abc import Mapping
from pathlib import Path

import pytest

from sentinel_bot.config import ConfigError, ServerConfig, Settings, load_settings

TOKEN_NAME = "SENTINEL_SERVER_NODE_TOKEN"
TOKEN = "synthetic_exporter_" + "x" * 32
ENV = {TOKEN_NAME: TOKEN}
ROOT = Path(__file__).resolve().parents[1]


def server_table(
    alias: str = "node",
    url: str = "https://node.example.test/metrics",
    extra: str = "",
) -> str:
    return (
        "\n[[servers]]\n"
        f"alias = {json.dumps(alias)}\nurl = {json.dumps(url)}\n"
        f"token_env = {json.dumps(TOKEN_NAME)}\n{extra}\n"
    )


def settings(
    tmp_path: Path,
    tables: str = "",
    *,
    mode: str = "monitor",
    environ: Mapping[str, str] | None = None,
) -> Settings:
    path = tmp_path / "config.toml"
    path.write_text(f'[runtime]\nmode = "{mode}"\n' + (tables or server_table()))
    return load_settings(path, ENV if environ is None else environ)


def server(
    tmp_path: Path,
    table: str = "",
    *,
    mode: str = "monitor",
    environ: Mapping[str, str] | None = None,
) -> ServerConfig:
    return settings(tmp_path, table, mode=mode, environ=environ).servers[0]


def test_https_server_has_independent_resource_rules_and_safe_defaults(tmp_path: Path) -> None:
    config = server(tmp_path)
    assert config.url == "https://node.example.test/metrics"
    assert config.token == TOKEN
    assert config.poll_interval == 30 and config.freshness_limit == 70
    assert [(rule.alias, rule.trigger, rule.recover) for rule in config.rules] == [
        ("node/exporter", None, None),
        ("node/cpu", 85, 75),
        ("node/memory", 85, 75),
    ]
    assert all(
        rule.failure_confirmation == 3 and rule.recovery_confirmation == 2 for rule in config.rules
    )
    assert TOKEN not in repr(config) and config.url not in repr(config)


@pytest.mark.parametrize(
    "url,networks",
    [
        ("https://10.20.0.10:9100/metrics", '["10.20.0.10/32"]'),
        ("https://192.168.10.5/metrics", '["192.168.10.0/24"]'),
        ("https://172.16.0.5/metrics", '["172.16.0.0/16"]'),
        ("https://[fd12:3456::5]:9100/metrics", '["fd12:3456::/64"]'),
        ("https://8.8.8.8/metrics", "[]"),
        ("https://[2001:4860:4860::8888]/metrics", "[]"),
    ],
)
def test_global_targets_or_explicit_private_networks_validate_without_dns(
    tmp_path: Path, url: str, networks: str
) -> None:
    config = server(tmp_path, server_table(url=url, extra=f"allowed_cidrs = {networks}"))
    assert config.url == url


@pytest.mark.parametrize(
    "url",
    [
        "http://node.example.test:9100/metrics",
        "https://127.0.0.1/metrics",
        "https://127.1/metrics",
        "https://2130706433/metrics",
        "https://0x7f000001/metrics",
        "https://0177.0.0.1/metrics",
        "https://0x7f.0.0.1/metrics",
        "https://localhost/metrics",
        "https://node.localhost/metrics",
        "https://169.254.169.254/metrics",
        "https://224.0.0.1/metrics",
        "https://0.0.0.0/metrics",
        "https://240.0.0.1/metrics",
        "https://10.20.0.10/metrics",
        "https://[::1]/metrics",
        "https://[::]/metrics",
        "https://[fe80::1]/metrics",
        "https://[ff02::1]/metrics",
        "https://[::ffff:8.8.8.8]/metrics",
        "https://[2001:4860::1%25eth0]/metrics",
        "https://user:password@node.example.test/metrics",
        "https://node.example.test/metrics?token=secret",
        "https://node.example.test/metrics#secret",
        "https://node.example.test\\attacker.example.test/metrics",
        "https://node.example.test:0/metrics",
        "https://node.example.test:65536/metrics",
        "https://node.example.test /metrics",
        "https:///metrics",
    ],
)
def test_unsafe_fixed_origin_targets_fail_before_workers_start(tmp_path: Path, url: str) -> None:
    with pytest.raises(ConfigError):
        server(tmp_path, server_table(url=url))


@pytest.mark.parametrize(
    "networks",
    [
        '["127.0.0.0/8"]',
        '["169.254.0.0/16"]',
        '["224.0.0.0/4"]',
        '["0.0.0.0/0"]',
        '["240.0.0.0/4"]',
        '["::1/128"]',
        '["fe80::/10"]',
        '["ff00::/8"]',
        '["::ffff:10.0.0.0/104"]',
        '["fd12::%eth0/64"]',
        '["10.20.0.1/24"]',
        '["10.20.0.0/24", "10.20.0.0/24"]',
        '["synthetic invalid network"]',
        "[1]",
        '"10.20.0.0/24"',
    ],
)
def test_address_allowlist_cannot_authorize_protected_or_ambiguous_networks(
    tmp_path: Path, networks: str
) -> None:
    with pytest.raises(ConfigError):
        server(tmp_path, server_table(extra=f"allowed_cidrs = {networks}"))


@pytest.mark.parametrize(
    "value", ["", "short", "x" * 129, "x" * 31, "x" * 32 + "\n", "bearer token", "x.y" * 16]
)
def test_enabled_exporter_requires_bounded_plain_bearer_token(tmp_path: Path, value: str) -> None:
    with pytest.raises(ConfigError) as error:
        server(tmp_path, environ={TOKEN_NAME: value})
    if value and not value.isspace():
        assert value not in str(error.value)


@pytest.mark.parametrize("reference", ["", "lowercase", "9TOKEN", "SENTINEL-KEY", "X" * 129])
def test_token_reference_is_validated_independently_of_value(
    tmp_path: Path, reference: str
) -> None:
    table = server_table().replace(json.dumps(TOKEN_NAME), json.dumps(reference))
    with pytest.raises(ConfigError):
        server(tmp_path, table)


def test_live_exporter_requires_environment_reference_and_never_inline_secret(
    tmp_path: Path,
) -> None:
    without_reference = server_table().replace(f'token_env = "{TOKEN_NAME}"\n', "")
    with pytest.raises(ConfigError):
        server(tmp_path, without_reference)
    with pytest.raises(ConfigError):
        server(tmp_path, server_table(extra='token = "synthetic_inline_secret"'))


def test_disabled_exporter_validates_structure_without_reading_its_secret(tmp_path: Path) -> None:
    class NoExporterSecrets(dict[str, str]):
        def get(self, key: str, default: object = None) -> object:
            if key == TOKEN_NAME:
                raise AssertionError("Disabled exporter must not inspect its secret")
            return super().get(key, default)

    config = settings(
        tmp_path, server_table(extra="enabled = false"), mode="idle", environ=NoExporterSecrets()
    )
    assert config.servers == () and config.polled_rules == ()
    with pytest.raises(ConfigError):
        settings(
            tmp_path,
            server_table(url="http://private.example.test/metrics", extra="enabled = false"),
            mode="idle",
            environ={},
        )


@pytest.mark.parametrize("port", [9100, 9101])
def test_demo_exporters_only_need_named_synthetic_service(tmp_path: Path, port: int) -> None:
    config = server(
        tmp_path,
        server_table(url=f"http://metrics-fixture:{port}/metrics"),
        mode="demo",
        environ={},
    )
    assert config.token == "" and config.url == f"http://metrics-fixture:{port}/metrics"


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost:9100/metrics",
        "http://127.0.0.1:9100/metrics",
        "http://site-fixture:9100/metrics",
        "http://metrics-fixture:9102/metrics",
        "https://metrics-fixture:9100/metrics",
        "https://node.example.test/metrics",
    ],
)
def test_demo_does_not_create_general_insecure_exporter_access(tmp_path: Path, url: str) -> None:
    with pytest.raises(ConfigError):
        server(tmp_path, server_table(url=url), mode="demo", environ={})


def test_informational_resources_retain_readings_without_threshold_rules(tmp_path: Path) -> None:
    config = server(
        tmp_path,
        server_table(
            extra='cpu_alerts = false\nmemory_alerts = false\n[[servers.filesystems]]\nalias = "root"\nmount = "/"\nalerts = false'
        ),
    )
    assert [rule.alias for rule in config.rules] == ["node/exporter"]
    assert config.mounts[0].trigger is None and config.mounts[0].recover is None
    assert config.mounts[0].mount == "/"


@pytest.mark.parametrize(
    "extra",
    [
        "cpu_trigger_percent = 75\ncpu_recovery_percent = 75",
        "memory_trigger_percent = 70\nmemory_recovery_percent = 75",
        "cpu_trigger_percent = nan",
        "memory_recovery_percent = inf",
        "cpu_trigger_percent = 101",
        "cpu_recovery_percent = -1",
        "memory_alerts = 1",
        "cpu_alerts = 'false'",
        "cpu_alerts = false\ncpu_recovery_percent = 99",
        "poll_interval_seconds = 0",
        "poll_interval_seconds = nan",
        "connect_timeout_seconds = 11\ntotal_timeout_seconds = 10",
        "total_timeout_seconds = 121",
        "failure_confirmation = 0",
        "recovery_confirmation = true",
        "reminder_interval_seconds = 0",
        "unknown = true",
    ],
)
def test_invalid_rule_and_scheduling_contracts_fail_startup(tmp_path: Path, extra: str) -> None:
    with pytest.raises(ConfigError):
        server(tmp_path, server_table(extra=extra))


@pytest.mark.parametrize(
    "filesystems",
    [
        'filesystems = [{ alias = "root", mount = "relative" }]',
        'filesystems = [{ alias = "root", mount = "" }]',
        'filesystems = [{ alias = "root", mount = "/bad\\npath" }]',
        'filesystems = [{ alias = "root", mount = "/' + "x" * 256 + '" }]',
        'filesystems = [{ alias = "path/root", mount = "/" }]',
        'filesystems = [{ alias = "root", mount = "/", alerts = 1 }]',
        'filesystems = [{ alias = "root", mount = "/", trigger_percent = 70, recovery_percent = 75 }]',
        'filesystems = [{ alias = "root", mount = "/", unknown = true }]',
        'filesystems = [{ alias = "root", mount = "/" }, { alias = "root", mount = "/var" }]',
        'filesystems = [{ alias = "root", mount = "/" }, { alias = "var", mount = "/" }]',
        "filesystems = ['root']",
        "filesystems = {}",
        "filesystems = ["
        + ", ".join('{alias="fs' + str(i) + '",mount="/data' + str(i) + '"}' for i in range(5))
        + "]",
    ],
)
def test_filesystem_selection_is_explicit_unique_and_bounded(
    tmp_path: Path, filesystems: str
) -> None:
    with pytest.raises(ConfigError):
        server(tmp_path, server_table(extra=filesystems))


def test_four_explicit_filesystems_create_separate_safe_rule_aliases(tmp_path: Path) -> None:
    mounts = (
        "filesystems = ["
        + ",".join(
            '{alias="data' + str(i) + '",mount="/synthetic/data' + str(i) + '"}' for i in range(4)
        )
        + "]"
    )
    config = server(tmp_path, server_table(extra=mounts))
    assert [rule.alias for rule in config.rules][-4:] == [f"node/fs_data{i}" for i in range(4)]
    assert "/synthetic/data" not in repr(config)


def test_token_rotation_and_timing_changes_preserve_incident_fingerprints(tmp_path: Path) -> None:
    initial = server(tmp_path)
    changed = server(
        tmp_path,
        server_table(extra="poll_interval_seconds = 60\nreminder_interval_seconds = 3600"),
        environ={TOKEN_NAME: "rotated_exporter_" + "z" * 32},
    )
    assert changed.fingerprint == initial.fingerprint
    assert {rule.alias: rule.fingerprint for rule in changed.rules} == {
        rule.alias: rule.fingerprint for rule in initial.rules
    }
    assert changed.rules[1].poll_interval == 60
    assert changed.rules[1].reminder_interval == 3600


def test_threshold_change_invalidates_only_affected_resource_rule(tmp_path: Path) -> None:
    initial = server(tmp_path)
    changed = server(
        tmp_path, server_table(extra="cpu_trigger_percent = 90\ncpu_recovery_percent = 80")
    )
    assert changed.fingerprint == initial.fingerprint
    before = {rule.alias: rule.fingerprint for rule in initial.rules}
    after = {rule.alias: rule.fingerprint for rule in changed.rules}
    assert [key for key in before if before[key] != after[key]] == ["node/cpu"]


def test_mount_change_invalidates_snapshot_and_only_its_filesystem_rule(tmp_path: Path) -> None:
    first = server_table(
        extra='filesystems = [{alias="root", mount="/"}, {alias="data", mount="/synthetic/data"}]'
    )
    initial = server(tmp_path, first)
    changed = server(tmp_path, first.replace("/synthetic/data", "/synthetic/new-data"))
    assert changed.fingerprint != initial.fingerprint
    before = {rule.alias: rule.fingerprint for rule in initial.rules}
    after = {rule.alias: rule.fingerprint for rule in changed.rules}
    assert [key for key in before if before[key] != after[key]] == ["node/fs_data"]


def test_exporter_origin_change_invalidates_all_its_rules(tmp_path: Path) -> None:
    initial = server(tmp_path)
    changed = server(tmp_path, server_table(url="https://other.example.test/metrics"))
    assert changed.fingerprint != initial.fingerprint
    assert all(
        first.fingerprint != second.fingerprint
        for first, second in zip(initial.rules, changed.rules, strict=True)
    )


def test_server_and_website_cannot_share_physical_alias(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        settings(tmp_path, '[[sites]]\nalias="node"\nurl="https://example.com"\n' + server_table())


def test_disabled_entries_still_count_toward_combined_twenty_source_inventory(
    tmp_path: Path,
) -> None:
    twenty = "".join(
        server_table(alias=f"node{i}", extra="cpu_alerts=false\nmemory_alerts=false\nenabled=false")
        for i in range(19)
    )
    twenty += server_table()
    config = settings(tmp_path, twenty)
    assert len(config.servers) == 1
    with pytest.raises(ConfigError, match="20 distinct"):
        settings(
            tmp_path,
            twenty + '[[sites]]\nalias="website"\nurl="https://example.com"\nenabled=false\n',
        )


def test_expanded_rules_allow_forty_and_reject_forty_one(tmp_path: Path) -> None:
    forty = "".join(server_table(alias=f"node{i}", extra="memory_alerts=false") for i in range(20))
    config = settings(tmp_path, forty)
    assert len(config.polled_rules) == 40 and len(config.polled_sources) == 20
    with pytest.raises(ConfigError, match="40 incident rules"):
        settings(tmp_path, forty.replace("memory_alerts=false", "memory_alerts=true", 1))


def ingress_tables(source_mode: str, *, source_alias: str = "node") -> str:
    return (
        '[email]\nenabled=true\nprovider="local_smtp"\n[ingress]\nenabled=true\n'
        "[[sources]]\n"
        f'alias="{source_alias}"\nmode="{source_mode}"\n'
        'allowed_types=["security.attack"]\ntoken_env="SENTINEL_SOURCE_NODE_TOKEN"\n'
    )


def ingress_env() -> dict[str, str]:
    return {
        **ENV,
        "SENTINEL_SOURCE_NODE_TOKEN": "source_" + "y" * 32,
        "SENTINEL_EMAIL_FROM": "sentinel@example.test",
        "SENTINEL_EMAIL_RECIPIENTS": "operator@example.test",
    }


def test_server_can_explicitly_receive_hybrid_events_without_duplicate_physical_source(
    tmp_path: Path,
) -> None:
    config = settings(tmp_path, server_table() + ingress_tables("hybrid"), environ=ingress_env())
    assert len(config.servers) == len(config.sources) == 1
    assert config.sources[0].mode == "hybrid"
    assert config.sources[0].alias == config.servers[0].alias


def test_push_only_source_cannot_create_exporter_probe_even_if_server_disabled(
    tmp_path: Path,
) -> None:
    for extra in ("", "enabled=false"):
        with pytest.raises(ConfigError, match="push-only"):
            settings(
                tmp_path, server_table(extra=extra) + ingress_tables("push"), environ=ingress_env()
            )


def test_push_only_inventory_has_no_server_rule_or_exporter_secret_requirement(
    tmp_path: Path,
) -> None:
    config = settings(tmp_path, ingress_tables("push"), environ=ingress_env())
    assert config.servers == () and config.polled_rules == () and config.polled_sources == ()
    assert config.sources[0].mode == "push"


@pytest.mark.parametrize("filename", ["servers.example.toml", "servers.local.toml"])
def test_public_server_examples_validate_offline(filename: str) -> None:
    settings = load_settings(ROOT / "config" / filename, ingress_env())
    assert settings.servers
    assert all(rule.alias.count("/") == 1 for rule in settings.polled_rules)
