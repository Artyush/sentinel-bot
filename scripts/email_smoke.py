"""Verify incident email with disposable Mailpit and loopback provider fixtures."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from docker_smoke import ROOT, Stack, messages, verify_container
from monitoring_smoke import MonitoringStack, sources, wait_status


class EmailStack(MonitoringStack):
    def __init__(self, name: str) -> None:
        Stack.__init__(self, name, local=True)
        self.base.extend(["-f", "compose.monitoring.yaml", "-f", "compose.email.yaml"])


def verify_mailpit(stack: EmailStack) -> None:
    rendered = json.loads(stack.compose("config", "--format", "json"))
    assert set(rendered["services"]) == {"sentinel", "site-fixture", "mailpit"}
    assert rendered["networks"]["sentinel"]["internal"] is True
    assert not rendered["services"]["sentinel"].get("ports")
    assert not rendered["services"]["site-fixture"].get("ports")
    stack.compose("up", "-d", "--wait", "--wait-timeout", "60")
    for service in ("sentinel", "site-fixture", "mailpit"):
        verify_container(stack, service)
    stack.execute("python", "-m", "sentinel_bot", "check-config")
    wait_status(
        stack,
        lambda value: all(row["state"] == "UP" and row["fresh"] for row in sources(value).values()),
    )
    address = stack.compose("port", "mailpit", "8025")
    assert address.startswith("127.0.0.1:")
    inbox_url = "http://" + address
    assert messages(inbox_url)["total"] == 0

    stack.control("primary", "down")
    down = wait_status(
        stack,
        lambda value: (
            sources(value)["primary"]["state"] == "DOWN" and value["email"]["accepted"] == 2
        ),
    )
    incident = sources(down)["primary"]["incident_id"]
    assert down["telegram"]["enabled"] is False
    assert down["email"]["pending"] == 0
    assert messages(inbox_url)["total"] == 2
    stack.control("primary", "up")
    recovered = wait_status(
        stack,
        lambda value: (
            sources(value)["primary"]["state"] == "UP" and value["email"]["accepted"] == 4
        ),
    )
    assert recovered["email"]["pending"] == 0
    inbox = messages(inbox_url)
    assert inbox["total"] == 4
    assert sum("DOWN" in item["Subject"] for item in inbox["messages"]) == 2
    assert sum("RECOVERY" in item["Subject"] for item in inbox["messages"]) == 2
    assert {recipient["Address"] for item in inbox["messages"] for recipient in item["To"]} == {
        "operator@example.test",
        "backup@example.test",
    }

    stack.compose("restart", "--timeout", "15", "sentinel")
    restarted = wait_status(
        stack,
        lambda value: sources(value)["primary"]["fresh"] and value["email"]["accepted"] == 4,
    )
    assert sources(restarted)["primary"]["incident_id"] is None
    assert isinstance(incident, str) and incident
    assert stack.execute("python", "-m", "sentinel_bot", "healthcheck") == "healthy"
    time.sleep(1.2)
    assert messages(inbox_url)["total"] == 4
    stack.compose("stop", "--timeout", "15", "sentinel")
    state = stack.inspect("sentinel")["State"]
    assert isinstance(state, dict) and state["ExitCode"] == 0
    print(
        "Mailpit incident smoke passed: independent synthetic recipients, DOWN/RECOVERY, "
        "email-only operation, accepted restart without resend, rootless/read-only and SIGTERM",
        flush=True,
    )


def verify_provider(stack: Stack) -> None:
    fixture = ROOT / "scripts" / "fixtures" / "email_app_smoke.py"
    environ = {
        **os.environ,
        "SENTINEL_CONTAINER_CONFIG": "/app/config.toml",
        "SENTINEL_TELEGRAM_BOT_TOKEN": "",
        "SENTINEL_TELEGRAM_CHAT_ID": "",
        "SENTINEL_TELEGRAM_ALLOWED_USER_IDS": "",
        "SENTINEL_TELEGRAM_TOPIC_ID": "",
        "SENTINEL_EMAIL_API_KEY": "",
        "SENTINEL_EMAIL_FROM": "",
        "SENTINEL_EMAIL_RECIPIENTS": "",
        "SENTINEL_SOURCE_APP_TOKEN": "",
        "SENTINEL_SERVER_NODE_TOKEN": "",
    }
    result = subprocess.run(
        [
            *stack.base,
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "--entrypoint",
            "python",
            "sentinel",
            "-",
        ],
        cwd=ROOT,
        env=environ,
        input=fixture.read_text(encoding="utf-8"),
        text=True,
        capture_output=True,
        timeout=90,
    )
    if result.returncode:
        raise RuntimeError(
            "The offline email application check failed:\n" + result.stdout + result.stderr
        )
    print(result.stdout.strip(), flush=True)


def main() -> None:
    name = "sentinel-email-smoke-" + uuid4().hex[:10]
    local = EmailStack(name)
    provider = Stack(name + "-provider", local=False)
    with TemporaryDirectory(prefix="sentinel-email-smoke-") as directory:
        override = Path(directory) / "isolated.yaml"
        override.write_text("networks:\n  sentinel:\n    internal: true\n", encoding="utf-8")
        provider.base.extend(["-f", str(override)])
        try:
            local.compose("config", "--quiet")
            provider.compose("config", "--quiet")
            print("Building the disposable email verification image", flush=True)
            local.compose("build", "sentinel", timeout=600)
            verify_mailpit(local)
            verify_provider(provider)
        except BaseException:
            print(
                local.compose("logs", "--tail", "30", "sentinel", "site-fixture", "mailpit"),
                flush=True,
            )
            raise
        finally:
            for stack in (local, provider):
                stack.compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
