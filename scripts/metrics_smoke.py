"""Verify Linux telemetry using disposable synthetic exporters and Mailpit."""

from __future__ import annotations

import json
import os
import subprocess
import time
from collections.abc import Callable
from uuid import uuid4

from docker_smoke import ROOT, Stack, messages, verify_container


class MetricsStack(Stack):
    def __init__(self, name: str) -> None:
        super().__init__(name, local=True)
        self.base.extend(["-f", "compose.metrics.yaml"])

    def status(self) -> dict[str, object]:
        return json.loads(self.execute("python", "-m", "sentinel_bot", "status"))

    def control(self, state: str) -> None:
        self.compose(
            "exec",
            "-T",
            "metrics-fixture",
            "python",
            "-c",
            "import sys,urllib.request; "
            "urllib.request.urlopen('http://127.0.0.1:9100/control',"
            "data=sys.argv[1].encode(),timeout=2).close()",
            state,
        )


def servers(snapshot: dict[str, object]) -> dict[str, dict[str, object]]:
    return {row["alias"]: row for row in snapshot["servers"]}


def rules(server: dict[str, object]) -> dict[str, dict[str, object]]:
    return {row["name"]: row for row in server["rules"]}


def wait_status(
    stack: MetricsStack, condition: Callable[[dict[str, object]], bool], timeout: float = 35
) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    latest: dict[str, object] = {}
    while time.monotonic() < deadline:
        latest = stack.status()
        if condition(latest):
            return latest
        time.sleep(0.1)
    raise AssertionError(f"Metrics status did not meet the condition: {latest!r}")


def verify_metrics(stack: MetricsStack) -> None:
    rendered = json.loads(stack.compose("config", "--format", "json"))
    assert set(rendered["services"]) == {"sentinel", "metrics-fixture", "mailpit"}
    assert rendered["networks"]["sentinel"]["internal"] is True
    assert not rendered["services"]["metrics-fixture"].get("ports")
    assert not rendered["services"]["sentinel"].get("ports")
    stack.compose("up", "-d", "--wait", "--wait-timeout", "90")
    for service in ("sentinel", "metrics-fixture", "mailpit"):
        verify_container(stack, service)
    stack.execute("python", "-m", "sentinel_bot", "check-config")
    initial = wait_status(
        stack,
        lambda value: all(
            row["availability"] == "UP" and row["fresh"] and row["cpu_percent"] is not None
            for row in servers(value).values()
        ),
    )
    assert initial["email"]["accepted"] == 0 and initial["pending_notifications"] == 0
    inbox_url = "http://" + stack.compose("port", "mailpit", "8025")
    assert messages(inbox_url)["total"] == 0

    stack.control("high")
    high = wait_status(
        stack,
        lambda value: (
            all(
                rules(servers(value)["primary"])[name]["state"] == "DOWN"
                for name in ("cpu", "memory", "fs_root")
            )
            and value["email"]["accepted"] == 6
        ),
    )
    opening_ids = {
        name: rules(servers(high)["primary"])[name]["incident_id"]
        for name in ("cpu", "memory", "fs_root")
    }
    assert all(isinstance(identity, str) and identity for identity in opening_ids.values())
    assert servers(high)["primary"]["availability"] == "UP"
    assert servers(high)["secondary"]["availability"] == "UP"
    assert servers(high)["secondary"]["fresh"] is True
    assert messages(inbox_url)["total"] == 6

    stack.control("between")
    between = wait_status(
        stack,
        lambda value: (
            79 < servers(value)["primary"]["memory_percent"] < 81
            and servers(value)["primary"]["cpu_percent"] is not None
            and 79 < servers(value)["primary"]["cpu_percent"] < 81
        ),
    )
    assert all(rules(servers(between)["primary"])[name]["state"] == "DOWN" for name in opening_ids)
    assert between["email"]["accepted"] == 6

    stack.control("missing")
    missing = wait_status(
        stack,
        lambda value: (
            servers(value)["primary"]["memory_percent"] is None
            and rules(servers(value)["primary"])["fs_root"]["state"] == "UP"
            and value["email"]["accepted"] == 8
        ),
    )
    assert rules(servers(missing)["primary"])["memory"]["state"] == "DOWN"
    assert rules(servers(missing)["primary"])["memory"]["fresh"] is False
    assert servers(missing)["primary"]["cpu_percent"] is not None
    assert servers(missing)["primary"]["filesystems"][0]["used_percent"] == 50

    stack.control("slow")
    unavailable = wait_status(
        stack,
        lambda value: (
            servers(value)["primary"]["availability"] == "DOWN" and value["email"]["accepted"] == 10
        ),
    )
    assert servers(unavailable)["secondary"]["availability"] == "UP"
    assert servers(unavailable)["secondary"]["fresh"] is True
    assert servers(unavailable)["primary"]["cpu_percent"] is None
    assert rules(servers(unavailable)["primary"])["cpu"]["incident_id"] == opening_ids["cpu"]
    assert rules(servers(unavailable)["primary"])["memory"]["incident_id"] == opening_ids["memory"]
    assert stack.execute("python", "-m", "sentinel_bot", "healthcheck") == "healthy"

    stack.compose("stop", "--timeout", "15", "sentinel")
    restored = json.loads(stack.compose("run", "--rm", "--no-deps", "sentinel", "status"))
    assert servers(restored)["primary"]["fresh"] is False
    assert rules(servers(restored)["primary"])["cpu"]["incident_id"] == opening_ids["cpu"]
    stack.compose("up", "-d", "--wait", "--wait-timeout", "60", "sentinel")
    stack.control("up")
    recovered = wait_status(
        stack,
        lambda value: (
            all(row["state"] == "UP" for row in rules(servers(value)["primary"]).values())
            and value["email"]["accepted"] == 16
        ),
    )
    assert recovered["email"]["pending"] == 0 and recovered["telegram"]["enabled"] is False
    inbox = messages(inbox_url)
    assert inbox["total"] == 16
    assert sum("DOWN" in item["Subject"] for item in inbox["messages"]) == 8
    assert sum("RECOVERY" in item["Subject"] for item in inbox["messages"]) == 8

    stack.python(
        "from datetime import UTC,datetime; from sentinel_bot.config import load_settings; "
        "from sentinel_bot.status import status_snapshot; "
        "from sentinel_bot.telegram_commands import Command,render_command; "
        "from pathlib import Path; import os; "
        "settings=load_settings(Path(os.environ['SENTINEL_CONFIG'])); "
        "snapshot=status_snapshot(settings); now=datetime.now(UTC); "
        "listing=render_command(Command('servers'),snapshot,now=now); "
        "detail=render_command(Command('server','primary'),snapshot,now=now); "
        "unknown=render_command(Command('server','not_configured'),snapshot,now=now); "
        "assert 'primary' in listing and 'secondary' in listing; "
        "assert 'primary' in detail and 'secondary' not in detail; "
        "assert 'Unknown server alias' in unknown; "
        "assert 'metrics-fixture' not in repr(snapshot); "
        "assert '/dev/synthetic' not in repr(snapshot); "
        "assert 'operator@example.test' not in repr(snapshot)"
    )
    stack.control("reset")
    wait_status(stack, lambda value: servers(value)["primary"]["cpu_percent"] is not None)
    assert messages(inbox_url)["total"] == 16
    stack.compose("restart", "--timeout", "15", "sentinel")
    wait_status(stack, lambda value: servers(value)["primary"]["cpu_percent"] is not None)
    assert messages(inbox_url)["total"] == 16
    stack.compose("stop", "--timeout", "15", "sentinel")
    state = stack.inspect("sentinel")["State"]
    assert isinstance(state, dict) and state["ExitCode"] == 0


def verify_application(stack: Stack) -> None:
    fixture = ROOT / "scripts" / "fixtures" / "metrics_app_smoke.py"
    result = subprocess.run(
        [*stack.base, "run", "--rm", "--no-deps", "-T", "--entrypoint", "python", "sentinel", "-"],
        cwd=ROOT,
        env={
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
        },
        input=fixture.read_text(encoding="utf-8"),
        text=True,
        capture_output=True,
        timeout=90,
    )
    if result.returncode:
        raise RuntimeError(
            "The metrics application smoke failed:\n" + result.stdout + result.stderr
        )
    print(result.stdout.strip(), flush=True)


def main() -> None:
    name = "sentinel-metrics-smoke-" + uuid4().hex[:10]
    stack = MetricsStack(name)
    provider = Stack(name + "-app", local=False)
    provider.base.extend(["-f", "compose.metrics.yaml"])
    try:
        stack.compose("config", "--quiet")
        print("Building the disposable Linux metrics verification image", flush=True)
        stack.compose("build", "sentinel", timeout=600)
        verify_metrics(stack)
        verify_application(provider)
        print(
            "Metrics smoke passed: isolated synthetic exporters, independent CPU/memory/filesystem "
            "incidents, hysteresis, unknown data, exporter availability, Mailpit acceptance, "
            "cached commands, CPU reset/restart, rootless/read-only, health and SIGTERM",
            flush=True,
        )
    except BaseException:
        print(
            stack.compose("logs", "--tail", "40", "sentinel", "metrics-fixture", "mailpit"),
            flush=True,
        )
        raise
    finally:
        for disposable in (stack, provider):
            disposable.compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
