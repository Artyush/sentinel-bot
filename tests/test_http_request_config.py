import json
from dataclasses import replace
from pathlib import Path

import pytest

from sentinel_bot.config import ConfigError, SiteConfig, load_settings

SITE = '[[sites]]\nalias = "endpoint"\nurl = "https://example.test/health"\n'


def inline_config(directory: Path, fields: str = "") -> Path:
    path = directory / "sentinel.toml"
    path.write_text('[runtime]\nmode = "monitor"\n' + SITE + fields, encoding="utf-8")
    return path


def external_config(
    directory: Path, content: str = SITE, *, reference: str = "checks.toml"
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    checks = directory / reference
    checks.parent.mkdir(parents=True, exist_ok=True)
    checks.write_text(content, encoding="utf-8")
    main = directory / "sentinel.toml"
    main.write_text(
        '[runtime]\nmode = "monitor"\nsites_file = ' + json.dumps(reference) + "\n",
        encoding="utf-8",
    )
    return main


def test_default_get_preserves_the_existing_fingerprint_contract(tmp_path: Path) -> None:
    legacy = SiteConfig("legacy", "https://example.test/")
    assert legacy.fingerprint == "cf97359b7fc353134ae9ed31a1dc5f764319f1ef988db692bbd521bf88a80776"
    assert legacy.method == "GET"
    assert legacy.request_body is None
    assert replace(legacy, method="GET").fingerprint == legacy.fingerprint
    loaded = load_settings(inline_config(tmp_path), {}).sites[0]
    assert loaded.method == "GET"
    assert loaded.request_body is None


def test_added_request_fields_preserve_existing_positional_construction() -> None:
    site = SiteConfig(
        "legacy", "https://example.test/", None, 200, 399, 30, 5, 10, 3, 2, 1800, 65536, True
    )
    assert site.enabled is True
    assert site.method == "GET"
    assert site.request_body is None


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_supported_methods_can_omit_a_body(tmp_path: Path, method: str) -> None:
    site = load_settings(inline_config(tmp_path, f'method = "{method}"\n'), {}).sites[0]
    assert site.method == method
    assert site.request_body is None


@pytest.mark.parametrize(
    "value", ['"get"', '"post"', '"HEAD"', '"DELETE"', '""', '"GET "', "true", "42", "[]", "{}"]
)
def test_invalid_methods_fail_configuration_without_echoing_value(
    tmp_path: Path, value: str
) -> None:
    with pytest.raises(ConfigError, match="GET or POST"):
        load_settings(inline_config(tmp_path, f"method = {value}\n"), {})


@pytest.mark.parametrize("explicit_method", ["", 'method = "GET"\n'])
def test_get_cannot_carry_even_an_empty_json_body(tmp_path: Path, explicit_method: str) -> None:
    with pytest.raises(ConfigError, match="requires POST"):
        load_settings(inline_config(tmp_path, explicit_method + "json_body = {}\n"), {})


def test_post_json_supports_nested_objects_lists_scalars_and_literal_unicode(
    tmp_path: Path,
) -> None:
    fields = (
        'method = "POST"\njson_body = {z = [1, true, "é"], a = {label = "healthy", ratio = 1.25}}\n'
    )
    site = load_settings(inline_config(tmp_path, fields), {}).sites[0]
    assert site.request_body == '{"a":{"label":"healthy","ratio":1.25},"z":[1,true,"é"]}'.encode()


def test_json_key_order_is_canonical_and_semantic_changes_invalidate_only_that_request(
    tmp_path: Path,
) -> None:
    first = load_settings(
        inline_config(tmp_path, 'method="POST"\njson_body={a=1,b={x=2,y=3}}\n'), {}
    ).sites[0]
    reordered = load_settings(
        inline_config(tmp_path, 'method="POST"\njson_body={b={y=3,x=2},a=1}\n'), {}
    ).sites[0]
    changed = load_settings(
        inline_config(tmp_path, 'method="POST"\njson_body={a=2,b={x=2,y=3}}\n'), {}
    ).sites[0]
    assert first.request_body == reordered.request_body
    assert first.fingerprint == reordered.fingerprint
    assert first.fingerprint != changed.fingerprint
    assert replace(first, method="GET", request_body=None).fingerprint != first.fingerprint
    assert replace(first, poll_interval=60, reminder_interval=3600).fingerprint == first.fingerprint


def test_empty_json_object_and_absent_post_body_are_distinct_requests(tmp_path: Path) -> None:
    omitted = load_settings(inline_config(tmp_path, 'method="POST"\n'), {}).sites[0]
    empty = load_settings(inline_config(tmp_path, 'method="POST"\njson_body={}\n'), {}).sites[0]
    assert omitted.request_body is None
    assert empty.request_body == b"{}"
    assert empty.fingerprint != omitted.fingerprint


@pytest.mark.parametrize("value", ['"text"', "[1,2]", "true", "42", "2026-01-01"])
def test_json_body_must_be_an_object(tmp_path: Path, value: str) -> None:
    with pytest.raises(ConfigError, match="TOML table"):
        load_settings(inline_config(tmp_path, f'method="POST"\njson_body={value}\n'), {})


@pytest.mark.parametrize(
    "value",
    [
        "{timestamp=2026-01-01T12:00:00Z}",
        "{date=2026-01-01}",
        "{time=12:00:00}",
        "{nested={value=nan}}",
        "{values=[1,inf]}",
        "{value=-inf}",
    ],
)
def test_toml_values_without_finite_json_representation_are_rejected(
    tmp_path: Path, value: str
) -> None:
    with pytest.raises(ConfigError, match="supported JSON values"):
        load_settings(inline_config(tmp_path, f'method="POST"\njson_body={value}\n'), {})


@pytest.mark.parametrize(
    ("text", "accepted"),
    [
        ("x" * 4085, True),
        ("x" * 4086, False),
        ("é" * 2042, True),
        ("é" * 2043, False),
        ("\n" * 2043, False),
    ],
)
def test_request_body_limit_counts_encoded_json_bytes(
    tmp_path: Path, text: str, accepted: bool
) -> None:
    fields = 'method="POST"\njson_body={text=' + json.dumps(text, ensure_ascii=False) + "}\n"
    path = inline_config(tmp_path, fields)
    if accepted:
        body = load_settings(path, {}).sites[0].request_body
        assert body is not None and len(body) <= 4096
        assert json.loads(body) == {"text": text}
    else:
        with pytest.raises(ConfigError, match="4096"):
            load_settings(path, {})


def test_request_payload_is_not_exposed_by_dataclass_repr_or_validation_errors(
    tmp_path: Path,
) -> None:
    secret = "synthetic-private-payload"
    site = load_settings(
        inline_config(tmp_path, f'method="POST"\njson_body={{value="{secret}"}}\n'), {}
    ).sites[0]
    assert secret not in repr(site)
    invalid = f'method="POST"\njson_body={{value="{secret}", invalid=nan}}\n'
    with pytest.raises(ConfigError) as raised:
        load_settings(inline_config(tmp_path, invalid), {})
    assert secret not in str(raised.value)
    assert secret not in repr(raised.value)


def test_disabled_requests_still_validate_method_and_body(tmp_path: Path) -> None:
    path = inline_config(tmp_path, 'enabled=false\nmethod="POST"\njson_body={invalid=nan}\n')
    with pytest.raises(ConfigError, match="supported JSON values"):
        load_settings(path, {})


def test_external_inventory_resolves_against_main_file_not_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "configuration"
    main = external_config(directory, reference="checks/endpoints.toml")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "checks").mkdir()
    (elsewhere / "checks/endpoints.toml").write_text("invalid", encoding="utf-8")
    monkeypatch.chdir(elsewhere)
    settings = load_settings(main, {"SENTINEL_STATE_DIR": str(tmp_path / "state")})
    assert settings.sites[0].alias == "endpoint"
    assert settings.sites[0].url == "https://example.test/health"
    assert not settings.state_dir.exists()


def test_external_inventory_keeps_per_endpoint_intervals_and_requests_independent(
    tmp_path: Path,
) -> None:
    content = (
        SITE + "poll_interval_seconds=15\n"
        '[[sites]]\nalias="api"\nurl="https://example.test/api"\n'
        'method="POST"\njson_body={check="readiness"}\npoll_interval_seconds=60\n'
    )
    settings = load_settings(external_config(tmp_path, content), {})
    assert [(site.alias, site.poll_interval, site.method) for site in settings.sites] == [
        ("endpoint", 15, "GET"),
        ("api", 60, "POST"),
    ]
    assert settings.sites[1].request_body == b'{"check":"readiness"}'


@pytest.mark.parametrize("inline", [SITE, "sites=[]\n"])
def test_external_inventory_cannot_be_mixed_with_any_inline_sites(
    tmp_path: Path, inline: str
) -> None:
    main = external_config(tmp_path)
    document = main.read_text(encoding="utf-8")
    if inline.startswith("sites="):
        document = inline + document
    else:
        document += inline
    main.write_text(document, encoding="utf-8")
    with pytest.raises(ConfigError, match="cannot be combined"):
        load_settings(main, {})


@pytest.mark.parametrize(
    "value",
    [
        '""',
        '" "',
        '"/absolute.toml"',
        '"../checks.toml"',
        '"checks/../checks.toml"',
        '"."',
        '"checks\\\\file.toml"',
        '"checks\\u0000.toml"',
        '"checks\\n.toml"',
        '"checks\\u007f.toml"',
        "true",
        "42",
        "[]",
        "{}",
        json.dumps("x" * 2049),
    ],
)
def test_external_inventory_path_is_bounded_relative_and_has_no_parent_traversal(
    tmp_path: Path, value: str
) -> None:
    main = tmp_path / "sentinel.toml"
    main.write_text('[runtime]\nmode="monitor"\nsites_file=' + value + "\n", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_settings(main, {})


@pytest.mark.parametrize(
    "reference", ["checks.toml", "./checks.toml", "nested/checks.toml", "checks with spaces.toml"]
)
def test_supported_relative_inventory_paths(tmp_path: Path, reference: str) -> None:
    assert len(load_settings(external_config(tmp_path, reference=reference), {}).sites) == 1


def test_inventory_symlink_cannot_escape_configuration_directory(tmp_path: Path) -> None:
    directory = tmp_path / "configuration"
    directory.mkdir()
    outside = tmp_path / "outside.toml"
    outside.write_text(SITE, encoding="utf-8")
    (directory / "checks.toml").symlink_to(outside)
    main = directory / "sentinel.toml"
    main.write_text('[runtime]\nmode="monitor"\nsites_file="checks.toml"\n', encoding="utf-8")
    with pytest.raises(ConfigError, match="within its configuration directory"):
        load_settings(main, {})


def test_symlink_to_inventory_inside_configuration_directory_is_supported(tmp_path: Path) -> None:
    main = external_config(tmp_path)
    (tmp_path / "alias.toml").symlink_to(tmp_path / "checks.toml")
    main.write_text('[runtime]\nmode="monitor"\nsites_file="alias.toml"\n', encoding="utf-8")
    assert len(load_settings(main, {}).sites) == 1


@pytest.mark.parametrize("missing", [True, False])
def test_missing_inventory_or_directory_is_a_safe_configuration_error(
    tmp_path: Path, missing: bool
) -> None:
    main = external_config(tmp_path)
    checks = tmp_path / "checks.toml"
    checks.unlink()
    if not missing:
        checks.mkdir()
    with pytest.raises(ConfigError, match="cannot be read"):
        load_settings(main, {})


@pytest.mark.parametrize(
    "content",
    [
        b"\xff",
        b"[sites",
        b"[runtime]\nmode='monitor'",
        b"",
        b"sites=true",
        b"[sites]\nalias='bad'",
        SITE.encode() + b"\n[telegram]\nenabled=false",
        SITE.encode() + b"\n[defaults]\nmethod='GET'",
    ],
)
def test_external_file_contains_only_a_valid_sites_inventory(
    tmp_path: Path, content: bytes
) -> None:
    main = external_config(tmp_path)
    (tmp_path / "checks.toml").write_bytes(content)
    with pytest.raises(ConfigError):
        load_settings(main, {})


def test_each_inventory_file_has_an_independent_sixteen_kib_limit(tmp_path: Path) -> None:
    main = external_config(tmp_path)
    checks = tmp_path / "checks.toml"
    initial = checks.read_bytes()
    checks.write_bytes(initial + b"#" + b"x" * (16384 - len(initial) - 1))
    assert len(load_settings(main, {}).sites) == 1
    checks.write_bytes(checks.read_bytes() + b"x")
    with pytest.raises(ConfigError, match="16 KiB"):
        load_settings(main, {})


def test_external_inventory_alias_and_disabled_row_limits_are_identical_to_inline(
    tmp_path: Path,
) -> None:
    content = "".join(
        f'[[sites]]\nalias="endpoint{index}"\nurl="https://example.test/"\nenabled={str(index == 0).lower()}\n'
        for index in range(20)
    )
    main = external_config(tmp_path, content)
    assert [site.alias for site in load_settings(main, {}).sites] == ["endpoint0"]
    with (tmp_path / "checks.toml").open("a", encoding="utf-8") as inventory:
        inventory.write('[[sites]]\nalias="extra"\nurl="https://example.test/"\nenabled=false\n')
    with pytest.raises(ConfigError, match="at most 20"):
        load_settings(main, {})
    (tmp_path / "checks.toml").write_text(SITE + SITE + "enabled=false\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="unique"):
        load_settings(main, {})


def test_external_disabled_rows_do_not_bypass_request_or_url_validation(tmp_path: Path) -> None:
    content = SITE + '[[sites]]\nalias="disabled"\nurl="http://127.0.0.1/"\nenabled=false\n'
    with pytest.raises(ConfigError, match="address/port"):
        load_settings(external_config(tmp_path, content), {})


def test_external_inventory_does_not_enable_recursive_includes(tmp_path: Path) -> None:
    main = external_config(tmp_path, SITE + '[runtime]\nsites_file="nested.toml"\n')
    with pytest.raises(ConfigError, match="only"):
        load_settings(main, {})


@pytest.mark.parametrize(
    ("mode", "content", "accepted"),
    [
        ("idle", "sites=[]\n", True),
        ("idle", SITE, False),
        ("monitor", "sites=[]\n", False),
        ("demo", SITE, False),
        (
            "demo",
            SITE.replace("https://example.test/health", "http://site-fixture:8080/health"),
            True,
        ),
    ],
)
def test_external_inventory_preserves_mode_isolation(
    tmp_path: Path, mode: str, content: str, accepted: bool
) -> None:
    main = external_config(tmp_path, content)
    main.write_text(
        main.read_text(encoding="utf-8").replace('mode = "monitor"', f'mode = "{mode}"'),
        encoding="utf-8",
    )
    if accepted:
        assert load_settings(main, {}).mode == mode
    else:
        with pytest.raises(ConfigError):
            load_settings(main, {})
