from pathlib import Path

import pytest

from sentinel_bot.config import ConfigError, load_settings


def write_config(directory: Path, content: str) -> Path:
    path = directory / "inventory.toml"
    path.write_text(content, encoding="utf-8")
    return path


def test_explicit_idle_settings_and_state_override(tmp_path: Path) -> None:
    path = write_config(tmp_path, '[runtime]\nmode = "idle"\n')
    state_dir = tmp_path / "state"

    settings = load_settings(path, {"SENTINEL_STATE_DIR": str(state_dir)})

    assert settings.mode == "idle"
    assert settings.state_dir == state_dir
    assert 0 < settings.heartbeat_interval < settings.health_timeout
    assert settings.shutdown_timeout > 0
    assert not state_dir.exists()


@pytest.mark.parametrize(
    "content",
    [
        "",
        "mode = 'idle'\n",
        "[runtime]\n",
        "[runtime]\nmode = 'live'\n",
        "[runtime]\nmode = 'demo'\n",
        "[runtime]\nmode = true\n",
        "[runtime]\nmode = 'idle'\nunknown = true\n",
        "[runtime]\nmode = 'idle'\n[telegram]\nenabled = true\n",
        "[runtime]\nmode = 'idle'\n[email]\nenabled = true\n",
        "[runtime]\nmode = 'idle'\n[ingress]\nenabled = true\n",
        "[runtime]\nmode = 'idle'\n[[sources]]\nmode = 'push'\n",
        "[runtime]\nmode = 'idle'\nheartbeat_interval_seconds = 0\n",
        "[runtime]\nmode = 'idle'\nheartbeat_interval_seconds = -1\n",
        "[runtime]\nmode = 'idle'\nheartbeat_interval_seconds = 0.01\n",
        "[runtime]\nmode = 'idle'\nheartbeat_interval_seconds = 61\n",
        "[runtime]\nmode = 'idle'\nheartbeat_interval_seconds = true\n",
        "[runtime]\nmode = 'idle'\nheartbeat_interval_seconds = nan\n",
        "[runtime]\nmode = 'idle'\nheartbeat_interval_seconds = " + "9" * 1000 + "\n",
        "[runtime]\nmode = 'idle'\nhealth_timeout_seconds = inf\n",
        "[runtime]\nmode = 'idle'\nhealth_timeout_seconds = 0\n",
        "[runtime]\nmode = 'idle'\nshutdown_timeout_seconds = 0\n",
        "[runtime]\nmode = 'idle'\nshutdown_timeout_seconds = '5'\n",
        "[runtime]\nmode = 'idle'\nshutdown_timeout_seconds = 31\n",
        "[runtime]\nmode = 'idle'\nhealth_timeout_seconds = 301\n",
        "[runtime]\nmode = 'idle'\nheartbeat_interval_seconds = 3\nhealth_timeout_seconds = 2\n",
        "[runtime]\nmode = 'idle'\nheartbeat_interval_seconds = 3\nhealth_timeout_seconds = 5\n",
        "[runtime]\nmode = 'idle'\nstate_dir = 1\n",
        "[runtime]\nmode = 'idle'\nstate_dir = ''\n",
    ],
)
def test_invalid_or_unimplemented_settings_are_rejected(tmp_path: Path, content: str) -> None:
    path = write_config(tmp_path, content)

    with pytest.raises(ConfigError):
        load_settings(path, {})


@pytest.mark.parametrize(
    "content",
    [
        "[runtime",
        "\udcff",
        '[runtime]\nmode="idle"\nstate_dir=' + "[" * 600 + "1" + "]" * 600,
    ],
)
def test_malformed_config_is_reported_as_config_error(tmp_path: Path, content: str) -> None:
    path = tmp_path / "inventory.toml"
    path.write_bytes(content.encode("utf-8", errors="surrogateescape"))

    with pytest.raises(ConfigError):
        load_settings(path, {})


def test_missing_and_oversized_config_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        load_settings(tmp_path / "missing.toml", {})

    oversized = write_config(tmp_path, "#" + "x" * (1024 * 1024))
    with pytest.raises(ConfigError):
        load_settings(oversized, {})


def test_invalid_value_is_not_included_in_error(tmp_path: Path) -> None:
    private_value = "synthetic-secret-do-not-print"
    path = write_config(tmp_path, f'[runtime]\nmode = "{private_value}"\n')

    with pytest.raises(ConfigError) as error:
        load_settings(path, {})

    assert private_value not in str(error.value)


@pytest.mark.parametrize("value", ["", "   ", "invalid\x00path", "x" * 2049])
def test_invalid_state_override_is_rejected(tmp_path: Path, value: str) -> None:
    path = write_config(tmp_path, '[runtime]\nmode = "idle"\n')

    with pytest.raises(ConfigError):
        load_settings(path, {"SENTINEL_STATE_DIR": value})


def test_existing_file_cannot_be_used_as_state_directory(tmp_path: Path) -> None:
    path = write_config(tmp_path, '[runtime]\nmode = "idle"\n')

    with pytest.raises(ConfigError):
        load_settings(path, {"SENTINEL_STATE_DIR": str(path)})


def test_custom_settings_at_safe_boundary(tmp_path: Path) -> None:
    path = write_config(
        tmp_path,
        "[runtime]\nmode = 'idle'\nheartbeat_interval_seconds = 0.1\n"
        "health_timeout_seconds = 0.2\nshutdown_timeout_seconds = 0.1\n",
    )

    settings = load_settings(path, {})

    assert settings.heartbeat_interval == 0.1
    assert settings.health_timeout == 0.2
    assert settings.shutdown_timeout == 0.1
