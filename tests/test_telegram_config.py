from pathlib import Path

import pytest

from sentinel_bot.config import ConfigError, load_settings

TOKEN = "123456789:synthetic_credential_abcdefghijklmnopqrstuvwxyz"
PREFIX = "SENTINEL_TELEGRAM_"


def environment() -> dict[str, str]:
    return {
        PREFIX + "BOT_TOKEN": TOKEN,
        PREFIX + "CHAT_ID": "-1001234567890",
        PREFIX + "ALLOWED_USER_IDS": "123456789,987654321",
        PREFIX + "TOPIC_ID": "42",
    }


def configuration(tmp_path: Path, *, mode: str = "idle", telegram: str = "enabled = true") -> Path:
    path = tmp_path / "inventory.toml"
    content = f'[runtime]\nmode = "{mode}"\nstate_dir = "{tmp_path / "state"}"\n'
    if mode != "idle":
        url = "http://site-fixture:8080/" if mode == "demo" else "https://example.test/"
        content += f'[[sites]]\nalias = "primary"\nurl = "{url}"\n'
    path.write_text(content + f"[telegram]\n{telegram}\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("mode", ["idle", "monitor"])
def test_enabled_telegram_has_one_fixed_destination_and_explicit_users(
    tmp_path: Path, mode: str
) -> None:
    settings = load_settings(configuration(tmp_path, mode=mode), environment())
    assert settings.telegram is not None
    assert settings.telegram.token == TOKEN
    assert settings.telegram.chat_id == -1001234567890
    assert settings.telegram.allowed_user_ids == frozenset({123456789, 987654321})
    assert settings.telegram.topic_id == 42
    assert settings.telegram.destination == "telegram:-1001234567890:42"
    assert not settings.state_dir.exists()


@pytest.mark.parametrize("value", [None, ""])
def test_private_chat_without_topic_is_supported(tmp_path: Path, value: str | None) -> None:
    env = environment()
    env[PREFIX + "CHAT_ID"] = "123456789"
    if value is None:
        del env[PREFIX + "TOPIC_ID"]
    else:
        env[PREFIX + "TOPIC_ID"] = value
    settings = load_settings(configuration(tmp_path), env)
    assert settings.telegram is not None
    assert settings.telegram.chat_id == 123456789
    assert settings.telegram.topic_id is None
    assert settings.telegram.destination == "telegram:123456789:0"


def test_group_topic_is_optional(tmp_path: Path) -> None:
    env = environment()
    del env[PREFIX + "TOPIC_ID"]
    settings = load_settings(configuration(tmp_path), env)
    assert settings.telegram is not None
    assert settings.telegram.topic_id is None


@pytest.mark.parametrize("field", ["BOT_TOKEN", "CHAT_ID", "ALLOWED_USER_IDS"])
def test_enabled_telegram_requires_credentials_and_access_settings(
    tmp_path: Path, field: str
) -> None:
    env = environment()
    del env[PREFIX + field]
    with pytest.raises(ConfigError):
        load_settings(configuration(tmp_path), env)


@pytest.mark.parametrize(
    "token",
    [
        "",
        "synthetic-secret",
        "123:short",
        "00001:" + "x" * 30,
        "123456789:" + "x" * 19,
        "123456789:" + "x" * 129,
        "123456789:" + "x" * 20 + "\n",
        "123456789:has spaces in synthetic token",
        "https://api.telegram.org/bot" + TOKEN,
    ],
)
def test_malformed_token_is_rejected_without_exposing_value(tmp_path: Path, token: str) -> None:
    env = environment()
    env[PREFIX + "BOT_TOKEN"] = token
    with pytest.raises(ConfigError) as raised:
        load_settings(configuration(tmp_path), env)
    if token:
        assert token not in str(raised.value)


@pytest.mark.parametrize(
    "identifier",
    ["", "0", "-0", "+1", "01", " 1", "1 ", "1.0", "true", "1\n", str(2**52), str(-(2**52))],
)
def test_chat_identifier_requires_canonical_bounded_numeric_value(
    tmp_path: Path, identifier: str
) -> None:
    env = environment()
    env[PREFIX + "CHAT_ID"] = identifier
    with pytest.raises(ConfigError):
        load_settings(configuration(tmp_path), env)


@pytest.mark.parametrize("identifier", [str(2**52 - 1), str(-(2**52 - 1))])
def test_chat_identifier_exact_supported_boundary(tmp_path: Path, identifier: str) -> None:
    env = environment()
    env[PREFIX + "CHAT_ID"] = identifier
    del env[PREFIX + "TOPIC_ID"]
    assert load_settings(configuration(tmp_path), env).telegram.chat_id == int(identifier)


@pytest.mark.parametrize(
    "users",
    [
        "",
        "0",
        "-1",
        "+1",
        "01",
        "true",
        "1.0",
        "1;2",
        "1,",
        ",1",
        "1,,2",
        "1,1",
        str(2**52),
        ",".join(str(value) for value in range(1, 22)),
        "1" * 401,
    ],
)
def test_user_allowlist_is_positive_unique_and_bounded(tmp_path: Path, users: str) -> None:
    env = environment()
    env[PREFIX + "ALLOWED_USER_IDS"] = users
    with pytest.raises(ConfigError):
        load_settings(configuration(tmp_path), env)


def test_twenty_unique_users_and_surrounding_whitespace_are_supported(tmp_path: Path) -> None:
    env = environment()
    env[PREFIX + "ALLOWED_USER_IDS"] = ", ".join(str(value) for value in range(1, 21))
    assert load_settings(configuration(tmp_path), env).telegram.allowed_user_ids == frozenset(
        range(1, 21)
    )


def test_user_identifier_exact_supported_boundary(tmp_path: Path) -> None:
    env = environment()
    env[PREFIX + "ALLOWED_USER_IDS"] = str(2**52 - 1)
    assert load_settings(configuration(tmp_path), env).telegram.allowed_user_ids == frozenset(
        {2**52 - 1}
    )


@pytest.mark.parametrize("topic", ["0", "-1", "true", "1.0", "01", "1\n", str(2**31), str(2**52)])
def test_topic_identifier_is_positive_and_bounded(tmp_path: Path, topic: str) -> None:
    env = environment()
    env[PREFIX + "TOPIC_ID"] = topic
    with pytest.raises(ConfigError):
        load_settings(configuration(tmp_path), env)


def test_maximum_supported_topic_identifier(tmp_path: Path) -> None:
    env = environment()
    env[PREFIX + "TOPIC_ID"] = str(2**31 - 1)
    assert load_settings(configuration(tmp_path), env).telegram.topic_id == 2**31 - 1


def test_private_chat_cannot_require_group_topic(tmp_path: Path) -> None:
    env = environment()
    env[PREFIX + "CHAT_ID"] = "123456789"
    with pytest.raises(ConfigError, match="group"):
        load_settings(configuration(tmp_path), env)


@pytest.mark.parametrize("telegram", ["", "enabled = false"])
def test_disabled_telegram_ignores_missing_and_malformed_environment(
    tmp_path: Path, telegram: str
) -> None:
    path = configuration(tmp_path, telegram=telegram)
    assert load_settings(path, {}).telegram is None
    env = {key: "malformed-synthetic-secret" for key in environment()}
    assert load_settings(path, env).telegram is None


def test_absent_telegram_section_needs_no_environment(tmp_path: Path) -> None:
    path = tmp_path / "inventory.toml"
    path.write_text('[runtime]\nmode = "idle"\n', encoding="utf-8")
    assert load_settings(path, {}).telegram is None


@pytest.mark.parametrize(
    "telegram",
    [
        'enabled = "true"',
        "enabled = 1",
        "enabled = []",
        'enabled = true\ntoken = "synthetic-secret"',
        "enabled = false\nchat_id = -100",
        "enabled = true\nallowed_user_ids = [1]",
        "enabled = true\ntopic_id = 1",
        'enabled = true\napi_url = "http://localhost"',
    ],
)
def test_telegram_table_accepts_only_boolean_enable_flag(tmp_path: Path, telegram: str) -> None:
    with pytest.raises(ConfigError):
        load_settings(configuration(tmp_path, telegram=telegram), environment())


@pytest.mark.parametrize("raw", ["telegram = true", "telegram = []", 'telegram = "enabled"'])
def test_telegram_section_requires_table(tmp_path: Path, raw: str) -> None:
    path = tmp_path / "inventory.toml"
    path.write_text(raw + '\n[runtime]\nmode = "idle"\n', encoding="utf-8")
    with pytest.raises(ConfigError):
        load_settings(path, environment())


@pytest.mark.parametrize("section", ["email", "ingress", "unknown"])
def test_other_unimplemented_sections_still_fail(tmp_path: Path, section: str) -> None:
    path = configuration(tmp_path)
    with path.open("a", encoding="utf-8") as file:
        file.write(f"[{section}]\nenabled = true\n")
    with pytest.raises(ConfigError):
        load_settings(path, environment())


def test_live_telegram_is_prohibited_in_isolated_demo(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="demo"):
        load_settings(configuration(tmp_path, mode="demo"), environment())
    settings = load_settings(configuration(tmp_path, mode="demo", telegram="enabled = false"), {})
    assert settings.mode == "demo"
    assert settings.telegram is None


def test_settings_repr_redacts_bot_and_destination_secrets(tmp_path: Path) -> None:
    env = environment()
    settings = load_settings(configuration(tmp_path), env)
    assert settings.telegram is not None
    for representation in (repr(settings), repr(settings.telegram)):
        for key, secret in env.items():
            if key == PREFIX + "TOPIC_ID":
                continue
            assert secret not in representation
        assert "123456789" not in representation
        assert "987654321" not in representation
        assert "topic_id" not in representation


def test_configuration_validation_never_contacts_telegram_or_creates_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def reject_network(*args: object, **kwargs: object) -> None:
        pytest.fail("Configuration must not contact external services")

    monkeypatch.setattr("socket.socket", reject_network)
    settings = load_settings(configuration(tmp_path, mode="monitor"), environment())
    assert settings.telegram is not None
    assert not settings.state_dir.exists()
    assert sorted(path.name for path in tmp_path.iterdir()) == ["inventory.toml"]
