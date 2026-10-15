"""Exercise disposable local and server Compose stacks without live credentials."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from urllib.request import urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]


def command(*args: str, expected: int = 0, timeout: float = 120) -> str:
    result = subprocess.run(
        args,
        cwd=ROOT,
        env={
            **os.environ,
            "MAILPIT_HTTP_PORT": "0",
            "SENTINEL_CONTAINER_CONFIG": "/app/config.toml",
            "SENTINEL_TELEGRAM_BOT_TOKEN": "",
            "SENTINEL_TELEGRAM_CHAT_ID": "",
            "SENTINEL_TELEGRAM_ALLOWED_USER_IDS": "",
            "SENTINEL_TELEGRAM_TOPIC_ID": "",
        },
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != expected:
        raise RuntimeError(
            f"Command {args!r} returned {result.returncode}, expected {expected}:\n"
            f"{result.stdout}{result.stderr}"
        )
    return result.stdout.strip()


class Stack:
    def __init__(self, name: str, *, local: bool) -> None:
        self.base = ["docker", "compose", "-p", name, "-f", "compose.yaml"]
        if local:
            self.base.extend(["-f", "compose.override.yaml"])

    def compose(self, *args: str, expected: int = 0, timeout: float = 120) -> str:
        return command(*self.base, *args, expected=expected, timeout=timeout)

    def execute(self, *args: str, expected: int = 0) -> str:
        return self.compose("exec", "-T", "sentinel", *args, expected=expected, timeout=20)

    def python(self, script: str, *, expected: int = 0) -> str:
        return self.execute("python", "-c", script, expected=expected)

    def inspect(self, service: str) -> dict[str, object]:
        container = self.compose("ps", "-a", "-q", service)
        return json.loads(command("docker", "inspect", container))[0]


def verify_container(stack: Stack, service: str) -> None:
    info = stack.inspect(service)
    host = info["HostConfig"]
    assert isinstance(host, dict)
    assert host["ReadonlyRootfs"] is True
    assert host["Privileged"] is False
    assert host["CapDrop"] == ["ALL"]
    assert "no-new-privileges:true" in host["SecurityOpt"]
    config = info["Config"]
    assert isinstance(config, dict) and config["User"] == "10001:10001"
    assert all(mount["Type"] != "bind" for mount in info["Mounts"])


def startup_metadata(stack: Stack) -> dict[str, object]:
    return json.loads(
        stack.python(
            "import json,sqlite3; "
            "db=sqlite3.connect('/var/lib/sentinel/state.sqlite3'); "
            "row=db.execute('SELECT first_started_at,last_started_at,startup_count "
            "FROM runtime_metadata').fetchone(); "
            "print(json.dumps(dict(zip(('first','last','count'),row)))); db.close()"
        )
    )


def messages(url: str) -> dict[str, object]:
    with urlopen(url + "/api/v1/messages", timeout=5) as response:
        return json.loads(response.read(1024 * 1024))


def verify_local(stack: Stack) -> None:
    stack.compose("up", "-d", "--wait", "--wait-timeout", "60")
    verify_container(stack, "sentinel")
    verify_container(stack, "mailpit")
    assert stack.execute("python", "-m", "sentinel_bot", "check-config")
    assert stack.execute("python", "-m", "sentinel_bot", "healthcheck") == "healthy"
    stack.python(
        "import os; assert os.getuid()==10001; assert os.stat('/var/lib/sentinel').st_uid==10001"
    )
    stack.python("open('/app/forbidden-write','w')", expected=1)
    stack.execute("python", "-m", "sentinel_bot", "run", expected=1)
    stack.execute(
        "python",
        "-m",
        "sentinel_bot",
        "--config",
        "/missing-config.toml",
        "check-config",
        expected=2,
    )
    address = stack.compose("port", "mailpit", "8025")
    assert address.startswith("127.0.0.1:")
    web = "http://" + address
    assert messages(web)["total"] == 0
    stack.execute("python", "-m", "sentinel_bot", "test-email")
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        inbox = messages(web)
        if inbox["total"] == 1:
            break
        time.sleep(0.1)
    else:
        raise AssertionError("The test email did not reach Mailpit")
    assert inbox["messages"][0]["Subject"] == "Sentinel Bot local email test"
    before = startup_metadata(stack)
    assert before["count"] == 1
    stack.compose("restart", "--timeout", "15", "sentinel")
    stack.compose("up", "-d", "--wait", "--wait-timeout", "60")
    after = startup_metadata(stack)
    assert after["first"] == before["first"]
    assert after["count"] == 2
    assert messages(web)["total"] == 1
    stack.compose("stop", "--timeout", "15", "sentinel")
    state = stack.inspect("sentinel")["State"]
    assert isinstance(state, dict) and state["ExitCode"] == 0
    stack.compose("run", "--rm", "--no-deps", "sentinel", "healthcheck", expected=1)


def verify_server(stack: Stack) -> None:
    config = json.loads(stack.compose("config", "--format", "json"))
    assert set(config["services"]) == {"sentinel"}
    assert not config["services"]["sentinel"].get("ports")
    stack.compose("up", "-d", "--wait", "--wait-timeout", "60")
    verify_container(stack, "sentinel")
    stack.execute("python", "-m", "sentinel_bot", "test-email", expected=1)
    assert stack.execute("python", "-m", "sentinel_bot", "healthcheck") == "healthy"
    stack.compose("stop", "--timeout", "15", "sentinel")
    state = stack.inspect("sentinel")["State"]
    assert isinstance(state, dict) and state["ExitCode"] == 0


def main() -> None:
    name = "sentinel-smoke-" + uuid4().hex[:10]
    local = Stack(name, local=True)
    server = Stack(name + "-server", local=False)
    try:
        local.compose("config", "--quiet")
        local.compose("build", "sentinel", timeout=600)
        verify_local(local)
        print(
            "Local smoke passed: rootless/read-only, state restart, health, SIGTERM, Mailpit email"
        )
        verify_server(server)
        print("Server smoke passed: same image, isolated state, no Mailpit or test-email")
    finally:
        for stack in (local, server):
            stack.compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
