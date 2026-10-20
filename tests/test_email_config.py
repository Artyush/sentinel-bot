from dataclasses import replace
from pathlib import Path

import pytest

from sentinel_bot.config import ConfigError, EmailSettings, load_settings

ENV = {
    "SENTINEL_EMAIL_API_KEY": "synthetic_credential_123456789",
    "SENTINEL_EMAIL_FROM": "sentinel@example.test",
    "SENTINEL_EMAIL_RECIPIENTS": "first@example.test, second@example.test",
}


def configured(tmp_path: Path, email: str, env: dict[str, str] | None = None) -> EmailSettings:
    path = tmp_path / "config.toml"
    path.write_text("[runtime]\nmode='idle'\n[email]\n" + email)
    result = load_settings(path, ENV if env is None else env).email
    assert result is not None
    return result


def test_mailchimp_uses_fixed_environment_routes_without_telegram(tmp_path: Path) -> None:
    email = configured(tmp_path, "enabled=true")
    assert email.provider == "mailchimp"
    assert email.request_timeout == 10
    assert email.recipients == ("first@example.test", "second@example.test")
    assert len(set(email.destinations)) == 2
    for destination, recipient in zip(email.destinations, email.recipients, strict=True):
        assert destination.startswith("email:") and len(destination) == 70
        assert "@" not in destination
        assert email.recipient_for(destination) == recipient
    with pytest.raises(ValueError, match="not configured"):
        email.recipient_for("email:" + "0" * 64)
    assert all(value not in repr(email) for value in ENV.values())


def test_route_identity_survives_key_rotation_order_and_deadline_changes(tmp_path: Path) -> None:
    email = configured(tmp_path, "enabled=true")
    assert (
        replace(email, api_key="rotated_credential", request_timeout=2).destinations
        == email.destinations
    )
    assert replace(email, recipients=tuple(reversed(email.recipients))).destinations == tuple(
        reversed(email.destinations)
    )
    assert set(replace(email, from_address="other@example.test").destinations).isdisjoint(
        email.destinations
    )
    assert set(replace(email, provider="local_smtp").destinations).isdisjoint(email.destinations)
    assert set(replace(email, recipients=("third@example.test",)).destinations).isdisjoint(
        email.destinations
    )


def test_local_smtp_ignores_unused_credentials_and_requires_only_synthetic_addresses(
    tmp_path: Path,
) -> None:
    email = configured(
        tmp_path,
        "enabled=true\nprovider='local_smtp'\nlocal_host='127.0.0.1'\nlocal_port=1025\nrequest_timeout_seconds=0.1",
        {key: value for key, value in ENV.items() if key != "SENTINEL_EMAIL_API_KEY"},
    )
    assert email.api_key == "" and email.local_host == "127.0.0.1"
    assert email.local_port == 1025 and email.request_timeout == 0.1
    assert replace(email, local_host="mailpit").destinations != email.destinations


def test_disabled_email_ignores_unused_environment(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text("[runtime]\nmode='idle'\n[email]\nenabled=false\nprovider='unused'")
    assert load_settings(path, {key: "\r\nprivate-invalid" for key in ENV}).email is None


@pytest.mark.parametrize(
    "table",
    [
        "enabled='true'",
        "enabled=1",
        "enabled=true\nprovider='smtp'",
        "enabled=true\nprovider=[]",
        "enabled=true\nrecipients=['injected@example.test']",
        "enabled=false\napi_key='inline-secret'",
        "enabled=true\nrequest_timeout_seconds=0",
        "enabled=true\nrequest_timeout_seconds=10.1",
        "enabled=true\nrequest_timeout_seconds=nan",
        "enabled=true\nrequest_timeout_seconds=true",
        "enabled=true\nlocal_host='localhost'",
        "enabled=true\nlocal_port=1025",
        "enabled=true\nprovider='local_smtp'\nlocal_host='smtp.example.test'",
        "enabled=true\nprovider='local_smtp'\nlocal_port=25",
        "enabled=true\nprovider='local_smtp'\nlocal_port=true",
    ],
)
def test_invalid_channel_configuration_is_rejected(tmp_path: Path, table: str) -> None:
    with pytest.raises(ConfigError):
        configured(tmp_path, table)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("SENTINEL_EMAIL_API_KEY", ""),
        ("SENTINEL_EMAIL_API_KEY", "short"),
        ("SENTINEL_EMAIL_API_KEY", "private\r\ncredential"),
        ("SENTINEL_EMAIL_API_KEY", "x" * 129),
        ("SENTINEL_EMAIL_FROM", ""),
        ("SENTINEL_EMAIL_FROM", "Sender <sender@example.test>"),
        ("SENTINEL_EMAIL_FROM", "sender@example.test\r\nBcc: other@example.test"),
        ("SENTINEL_EMAIL_FROM", "ü@example.test"),
        ("SENTINEL_EMAIL_FROM", ".first@example.test"),
        ("SENTINEL_EMAIL_FROM", "first..last@example.test"),
        ("SENTINEL_EMAIL_FROM", "x" * 65 + "@example.test"),
        ("SENTINEL_EMAIL_RECIPIENTS", ""),
        ("SENTINEL_EMAIL_RECIPIENTS", "first@example.test,"),
        ("SENTINEL_EMAIL_RECIPIENTS", "first@example.test,FIRST@EXAMPLE.TEST"),
        ("SENTINEL_EMAIL_RECIPIENTS", "first@example.test\n"),
        ("SENTINEL_EMAIL_RECIPIENTS", "x" * 1280),
        ("SENTINEL_EMAIL_RECIPIENTS", ",".join(f"operator{i}@example.test" for i in range(6))),
    ],
)
def test_credentials_and_routes_are_bounded_and_safe(tmp_path: Path, key: str, value: str) -> None:
    with pytest.raises(ConfigError) as caught:
        configured(tmp_path, "enabled=true", ENV | {key: value})
    assert "private" not in str(caught.value)


@pytest.mark.parametrize("key", ["SENTINEL_EMAIL_FROM", "SENTINEL_EMAIL_RECIPIENTS"])
def test_local_sink_cannot_receive_real_domain_addresses(tmp_path: Path, key: str) -> None:
    with pytest.raises(ConfigError, match="synthetic"):
        configured(
            tmp_path, "enabled=true\nprovider='local_smtp'", ENV | {key: "operator@example.com"}
        )


@pytest.mark.parametrize("provider", ["local_smtp", "mailchimp"])
def test_demo_allows_only_synthetic_local_email(tmp_path: Path, provider: str) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        "[runtime]\nmode='demo'\n[[sites]]\nalias='fixture'\nurl='http://site-fixture:8080/'\n"
        f"[email]\nenabled=true\nprovider='{provider}'\n"
    )
    if provider == "mailchimp":
        with pytest.raises(ConfigError, match="synthetic"):
            load_settings(path, ENV)
    else:
        assert load_settings(path, ENV).email is not None


def test_push_only_source_can_start_with_email_and_no_telegram_credentials(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        "[runtime]\nmode='monitor'\n[email]\nenabled=true\nprovider='local_smtp'\n"
        "[ingress]\nenabled=true\n[[sources]]\nalias='security'\nmode='push'\n"
        "allowed_types=['security.attack']\ntoken_env='SYNTHETIC_SOURCE_TOKEN'\n"
    )
    settings = load_settings(path, ENV | {"SYNTHETIC_SOURCE_TOKEN": "x" * 32})
    assert settings.email is not None and settings.telegram is None
    assert settings.sites == () and settings.sources[0].mode == "push"
