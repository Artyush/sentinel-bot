import os
import subprocess
import sys
from pathlib import Path

import pytest

from sentinel_bot.__main__ import main


def run_cli(
    tmp_path: Path, *arguments: str, environment: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    project_root = Path(__file__).resolve().parents[1]
    child_environment = {
        key: value for key, value in os.environ.items() if not key.startswith("SENTINEL_")
    }
    child_environment["PYTHONPATH"] = str(project_root / "src")
    child_environment.update(environment or {})
    return subprocess.run(
        [sys.executable, "-m", "sentinel_bot", *arguments],
        cwd=tmp_path,
        env=child_environment,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def test_check_config_accepts_environment_path_without_creating_state(tmp_path: Path) -> None:
    path = tmp_path / "idle.toml"
    path.write_text('[runtime]\nmode = "idle"\n', encoding="utf-8")
    state_dir = tmp_path / "state"

    result = run_cli(
        tmp_path,
        "check-config",
        environment={"SENTINEL_CONFIG": str(path), "SENTINEL_STATE_DIR": str(state_dir)},
    )

    assert result.returncode == 0, result.stderr
    assert "Configuration is valid" in result.stdout
    assert result.stderr == ""
    assert not state_dir.exists()


def test_config_argument_overrides_environment_path(tmp_path: Path) -> None:
    path = tmp_path / "idle.toml"
    path.write_text('[runtime]\nmode = "idle"\n', encoding="utf-8")

    result = run_cli(
        tmp_path,
        "--config",
        str(path),
        "check-config",
        environment={"SENTINEL_CONFIG": str(tmp_path / "missing.toml")},
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("command", ["check-config", "run", "healthcheck", "test-email"])
def test_invalid_configuration_has_safe_error_for_every_command(
    tmp_path: Path, command: str
) -> None:
    private_value = "synthetic-secret-do-not-print"
    path = tmp_path / "invalid.toml"
    path.write_text(f'[runtime]\nmode = "{private_value}"\n', encoding="utf-8")

    result = run_cli(tmp_path, "--config", str(path), command)

    assert result.returncode == 2
    assert "Configuration error:" in result.stderr
    assert private_value not in result.stderr + result.stdout
    assert "Traceback" not in result.stderr


def test_healthcheck_before_start_is_unhealthy_without_creating_state(tmp_path: Path) -> None:
    path = tmp_path / "idle.toml"
    path.write_text('[runtime]\nmode = "idle"\n', encoding="utf-8")
    state_dir = tmp_path / "state"

    result = run_cli(
        tmp_path,
        "--config",
        str(path),
        "healthcheck",
        environment={"SENTINEL_STATE_DIR": str(state_dir)},
    )

    assert result.returncode == 1
    assert not state_dir.exists()


def test_email_command_requires_opt_in(tmp_path: Path) -> None:
    path = tmp_path / "idle.toml"
    path.write_text('[runtime]\nmode = "idle"\n', encoding="utf-8")

    result = run_cli(tmp_path, "--config", str(path), "test-email")

    assert result.returncode == 1
    assert "Email test failed:" in result.stderr
    assert "Traceback" not in result.stderr


def test_help_describes_available_commands_without_loading_configuration(tmp_path: Path) -> None:
    result = run_cli(tmp_path, "--help")

    assert result.returncode == 0
    for command in ("check-config", "run", "healthcheck", "test-email"):
        assert command in result.stdout


def test_unknown_command_is_not_accepted(tmp_path: Path) -> None:
    result = run_cli(tmp_path, "unsupported-command")

    assert result.returncode == 2
    assert "invalid choice" in result.stderr


@pytest.mark.parametrize(("command", "status"), [("check-config", 0), ("healthcheck", 1)])
def test_read_only_commands_make_no_network_requests_when_smtp_is_opted_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str, status: int
) -> None:
    path = tmp_path / "idle.toml"
    path.write_text('[runtime]\nmode = "idle"\n', encoding="utf-8")
    monkeypatch.setenv("SENTINEL_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("SENTINEL_SMTP_TEST_ENABLED", "true")
    monkeypatch.setenv("SENTINEL_SMTP_HOST", "127.0.0.1")

    def forbidden_connection(*args: object, **kwargs: object) -> None:
        pytest.fail("configuration and health commands must not connect to external services")

    monkeypatch.setattr("socket.create_connection", forbidden_connection)

    assert main(["--config", str(path), command]) == status


def test_unexpected_runtime_error_remains_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "idle.toml"
    path.write_text('[runtime]\nmode = "idle"\n', encoding="utf-8")

    async def fail_runtime(settings: object) -> None:
        raise RuntimeError("synthetic unexpected worker failure")

    monkeypatch.setattr("sentinel_bot.__main__.run", fail_runtime)

    with pytest.raises(RuntimeError, match="synthetic unexpected worker failure"):
        main(["--config", str(path), "run"])
