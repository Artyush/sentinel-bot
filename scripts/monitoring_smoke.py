"""Verify website incidents with disposable, isolated Docker fixtures."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from uuid import uuid4

from docker_smoke import Stack, verify_container


class MonitoringStack(Stack):
    def __init__(self, name: str) -> None:
        super().__init__(name, local=False)
        self.base.extend(["-f", "compose.monitoring.yaml"])

    def status(self) -> dict[str, object]:
        return json.loads(self.execute("python", "-m", "sentinel_bot", "status"))

    def control(self, alias: str, state: str) -> None:
        self.compose(
            "exec",
            "-T",
            "site-fixture",
            "python",
            "-c",
            "import sys,urllib.request; "
            "request=urllib.request.Request('http://127.0.0.1:8080/control/'+sys.argv[1], "
            "data=sys.argv[2].encode(),method='POST'); "
            "urllib.request.urlopen(request,timeout=2).close()",
            alias,
            state,
        )

    def queue(self) -> list[dict[str, object]]:
        return json.loads(
            self.python(
                "import json; from pathlib import Path; "
                "from sentinel_bot.storage import SQLiteStore; "
                "store=SQLiteStore(Path('/var/lib/sentinel')); "
                "print(json.dumps([{'kind':item.event.kind.value, "
                "'incident_id':item.event.incident_id, 'source_alias':item.source_alias} "
                "for item in store.pending_notifications()]))"
            )
        )


def sources(status: dict[str, object]) -> dict[str, dict[str, object]]:
    rows = status["sites"]
    assert isinstance(rows, list)
    return {row["alias"]: row for row in rows}


def wait_status(
    stack: MonitoringStack, condition: Callable[[dict[str, object]], bool], timeout: float = 20
) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    latest: dict[str, object] = {}
    while time.monotonic() < deadline:
        latest = stack.status()
        if condition(latest):
            return latest
        time.sleep(0.1)
    raise AssertionError(f"Monitoring status did not meet the condition: {latest!r}")


def verify_monitoring(stack: MonitoringStack) -> None:
    rendered = json.loads(stack.compose("config", "--format", "json"))
    assert set(rendered["services"]) == {"sentinel", "site-fixture"}
    assert rendered["networks"]["sentinel"]["internal"] is True
    assert all(not service.get("ports") for service in rendered["services"].values())
    stack.compose("up", "-d", "--wait", "--wait-timeout", "60")
    verify_container(stack, "sentinel")
    verify_container(stack, "site-fixture")
    stack.execute("python", "-m", "sentinel_bot", "check-config")
    initial = wait_status(
        stack,
        lambda value: all(row["state"] == "UP" and row["fresh"] for row in sources(value).values()),
    )
    assert initial["pending_notifications"] == 0

    stack.control("primary", "slow")
    down = wait_status(
        stack,
        lambda value: (
            sources(value)["primary"]["state"] == "DOWN"
            and sources(value)["secondary"]["state"] == "UP"
            and sources(value)["secondary"]["fresh"]
        ),
    )
    incident = sources(down)["primary"]["incident_id"]
    assert isinstance(incident, str) and incident
    assert down["pending_notifications"] == 1
    assert stack.queue() == [{"kind": "DOWN", "incident_id": incident, "source_alias": "primary"}]
    assert stack.execute("python", "-m", "sentinel_bot", "healthcheck") == "healthy"

    stack.compose("restart", "--timeout", "15", "sentinel")
    restored = stack.status()
    assert sources(restored)["primary"]["state"] == "DOWN"
    assert sources(restored)["primary"]["incident_id"] == incident
    assert sources(restored)["primary"]["fresh"] is False
    assert restored["pending_notifications"] == 1
    continued = wait_status(
        stack,
        lambda value: sources(value)["primary"]["fresh"] and sources(value)["secondary"]["fresh"],
    )
    assert sources(continued)["primary"]["incident_id"] == incident
    assert continued["pending_notifications"] == 1

    stack.control("primary", "up")
    recovered = wait_status(
        stack,
        lambda value: (
            sources(value)["primary"]["state"] == "UP"
            and sources(value)["primary"]["fresh"]
            and value["pending_notifications"] == 2
        ),
    )
    assert sources(recovered)["primary"]["incident_id"] is None
    assert stack.queue() == [
        {"kind": "DOWN", "incident_id": incident, "source_alias": "primary"},
        {"kind": "RECOVERY", "incident_id": incident, "source_alias": "primary"},
    ]
    stack.compose("stop", "--timeout", "15", "sentinel")
    state = stack.inspect("sentinel")["State"]
    assert isinstance(state, dict) and state["ExitCode"] == 0


def main() -> None:
    stack = MonitoringStack("sentinel-monitoring-smoke-" + uuid4().hex[:10])
    try:
        stack.compose("config", "--quiet")
        print("Building the disposable website monitoring image", flush=True)
        stack.compose("build", "sentinel", timeout=600)
        print("Image built; starting the isolated monitoring checks", flush=True)
        verify_monitoring(stack)
        print(
            "Monitoring smoke passed: isolated sources, DOWN/RECOVERY, atomic queue, "
            "stale restart, incident identity, non-root/read-only, health and SIGTERM"
        )
    except BaseException:
        print(stack.compose("logs", "--tail", "30", "sentinel", "site-fixture"), flush=True)
        raise
    finally:
        stack.compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
