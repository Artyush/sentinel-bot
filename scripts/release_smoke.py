"""Verify the combined offline demo from an isolated copy of public repository files."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import ProxyHandler, build_opener
from uuid import uuid4

from docker_smoke import ROOT, Stack, startup_metadata, verify_container
from metrics_smoke import rules, servers
from monitoring_smoke import sources

NEW_RELEASE_FILES = (
    "LICENSE",
    "compose.release.yaml",
    "config/release.local.toml",
    "src/sentinel_bot/demo.py",
    "demo/telegram_fixture.py",
    "demo/control.py",
    "scripts/release_smoke.py",
    "docs/demo.md",
    "tests/test_demo_telegram_fixture.py",
    "tests/test_release_demo.py",
)
SERVICES = ("sentinel", "site-fixture", "metrics-fixture", "telegram-fixture", "mailpit")


def export_checkout(destination: Path) -> None:
    tracked = (
        subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            timeout=10,
        )
        .stdout.decode()
        .split("\0")
    )
    paths = set(filter(None, tracked)) | set(NEW_RELEASE_FILES)
    assert ".env" not in paths and "AGENTS.md" not in paths
    copied = 0
    for name in sorted(paths):
        source = ROOT / name
        if not source.exists():
            continue
        assert source.is_file() and not source.is_symlink(), name
        relative = Path(name)
        assert not relative.is_absolute() and ".." not in relative.parts
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        copied += 1
    assert (destination / ".env.example").is_file()
    assert (destination / "config/release.local.toml").is_file()
    assert not (destination / ".env").exists() and not (destination / ".git").exists()
    assert not (destination / "docs/roadmap.md").exists()
    print(f"Exported {copied} public files into an empty candidate checkout", flush=True)


class ReleaseStack(Stack):
    def __init__(self, root: Path, name: str) -> None:
        super().__init__(name, local=True)
        self.root = root
        self.image = name + ":local"
        self.paused = False
        self.environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("SENTINEL_")
            and key
            not in {
                "COMPOSE_FILE",
                "COMPOSE_ENV_FILES",
                "COMPOSE_PROFILES",
                "COMPOSE_PROJECT_NAME",
                "COMPOSE_PATH_SEPARATOR",
            }
        }
        self.environment.update(MAILPIT_HTTP_PORT="0", TELEGRAM_DEMO_HTTP_PORT="0")
        image_override = root / ".release-smoke-image.yaml"
        image_override.write_text(
            "services:\n"
            + "".join(
                f"  {service}:\n    image: {self.image}\n"
                for service in SERVICES
                if service != "mailpit"
            ),
            encoding="utf-8",
        )
        self.base.extend(
            [
                "-f",
                "compose.monitoring.yaml",
                "-f",
                "compose.metrics.yaml",
                "-f",
                "compose.release.yaml",
                "-f",
                str(image_override),
            ]
        )

    def command(self, *arguments: str, expected: int = 0, timeout: float = 120) -> str:
        result = subprocess.run(
            arguments,
            cwd=self.root,
            env=self.environment,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        if result.returncode != expected:
            raise RuntimeError(
                f"Command {arguments!r} returned {result.returncode}, expected {expected}:\n"
                + result.stdout
                + result.stderr
            )
        return result.stdout.strip()

    def compose(self, *args: str, expected: int = 0, timeout: float = 120) -> str:
        return self.command(*self.base, *args, expected=expected, timeout=timeout)

    def inspect(self, service: str) -> dict[str, object]:
        container = self.compose("ps", "-a", "-q", service)
        return json.loads(self.command("docker", "inspect", container))[0]

    def status(self) -> dict[str, object]:
        return json.loads(self.execute("python", "-m", "sentinel_bot", "status"))

    def request(
        self,
        service: str,
        url: str,
        body: str,
        *,
        source_auth: bool = False,
    ) -> dict[str, object]:
        script = (
            "import json,sys,urllib.error,urllib.request; "
            "from sentinel_bot.demo import DEMO_SOURCE_TOKEN; "
            "headers={'Content-Type':'application/json'}; "
            "headers.update({'Authorization':'Bearer '+DEMO_SOURCE_TOKEN} "
            "if sys.argv[3]=='auth' else {}); "
            "request=urllib.request.Request(sys.argv[1],data=sys.argv[2].encode(),"
            "headers=headers,method='POST'); "
            "opener=urllib.request.build_opener(urllib.request.ProxyHandler({})); "
            "\ntry:\n response=opener.open(request,timeout=5)"
            "\nexcept urllib.error.HTTPError as error:\n response=error"
            "\nwith response:\n "
            "print(json.dumps({'status':response.status,'body':response.read(65536).decode()}))"
        )
        return json.loads(
            self.compose(
                "exec",
                "-T",
                service,
                "python",
                "-c",
                script,
                url,
                body,
                "auth" if source_auth else "none",
                timeout=15,
            )
        )

    def helper(self, *arguments: str) -> str:
        return self.compose(
            "exec", "-T", "telegram-fixture", "python", "/app/demo/control.py", *arguments
        )

    def saved_event(self) -> dict[str, object]:
        return json.loads(
            self.compose(
                "exec",
                "-T",
                "telegram-fixture",
                "python",
                "-c",
                "from pathlib import Path\n"
                "with Path('/tmp/sentinel-demo-event.json').open('rb') as source:\n"
                "    body=source.read(16385)\n"
                "assert len(body)<=16384; print(body.decode())",
            )
        )

    def event(self, payload: dict[str, object], *, expected: int = 202) -> dict[str, object]:
        result = self.request(
            "telegram-fixture",
            "http://sentinel:8080/api/v1/events",
            json.dumps(payload),
            source_auth=True,
        )
        assert result["status"] == expected, result
        return json.loads(result["body"])

    def inbox_url(self, service: str, port: int) -> str:
        address = self.compose("port", service, str(port))
        assert address.startswith("127.0.0.1:")
        return "http://" + address


def http_json(url: str) -> dict[str, object]:
    assert url.startswith("http://127.0.0.1:")
    with build_opener(ProxyHandler({})).open(url, timeout=5) as response:
        body = response.read(1024 * 1024 + 1)
    assert len(body) <= 1024 * 1024
    return json.loads(body)


def wait_status(
    stack: ReleaseStack,
    condition: Callable[[dict[str, object]], bool],
    *,
    timeout: float = 60,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    latest: dict[str, object] = {}
    while time.monotonic() < deadline:
        latest = stack.status()
        if condition(latest):
            return latest
        time.sleep(0.2)
    raise AssertionError(f"Release status did not meet the condition: {latest!r}")


def delivered(snapshot: dict[str, object], telegram: int, email: int) -> bool:
    return (
        snapshot["telegram"]["accepted"] == telegram
        and snapshot["email"]["accepted"] == email
        and snapshot["telegram"]["pending"] == 0
        and snapshot["email"]["pending"] == 0
        and snapshot["telegram"]["in_flight"] == 0
        and snapshot["email"]["in_flight"] == 0
    )


def verify_release(stack: ReleaseStack) -> None:
    rendered = json.loads(stack.compose("config", "--format", "json"))
    assert set(rendered["services"]) == set(SERVICES)
    assert rendered["networks"]["sentinel"]["internal"] is True
    for service in ("sentinel", "site-fixture", "metrics-fixture"):
        assert not rendered["services"][service].get("ports")
    for service in ("mailpit", "telegram-fixture"):
        assert all(
            port["host_ip"] == "127.0.0.1" for port in rendered["services"][service]["ports"]
        )
    assert {port["target"] for port in rendered["services"]["telegram-fixture"]["ports"]} == {8081}
    startup_started = time.monotonic()
    stack.compose("up", "-d", "--build", "--wait", "--wait-timeout", "120", timeout=600)
    print(
        f"One-command build and five-service startup passed in "
        f"{time.monotonic() - startup_started:.1f}s",
        flush=True,
    )
    for service in SERVICES:
        verify_container(stack, service)
    assert stack.execute("python", "-m", "sentinel_bot", "check-config")
    initial = wait_status(
        stack,
        lambda value: (
            all(row["state"] == "UP" and row["fresh"] for row in sources(value).values())
            and all(
                row["availability"] == "UP" and row["fresh"] and row["cpu_percent"] is not None
                for row in servers(value).values()
            )
        ),
    )
    assert set(sources(initial)) == {"primary", "secondary"}
    assert set(servers(initial)) == {"node", "peer"}
    assert initial["sources"][0]["alias"] == "edge"
    assert initial["sources"][0]["availability"] == "UNKNOWN"
    assert delivered(initial, 0, 0)
    mail_url = stack.inbox_url("mailpit", 8025)
    telegram_url = stack.inbox_url("telegram-fixture", 8081)
    assert http_json(mail_url + "/api/v1/messages")["total"] == 0
    assert http_json(telegram_url + "/api/messages")["accepted_count"] == 0
    assert json.loads(stack.helper("inbox"))["accepted_count"] == 0
    rejected = stack.request("telegram-fixture", "http://sentinel:8080/api/v1/events", "not JSON")
    assert rejected["status"] == 401

    stack.helper("site", "primary", "slow")
    stack.helper("metrics", "memory-high")
    down = wait_status(
        stack,
        lambda value: (
            sources(value)["primary"]["state"] == "DOWN"
            and rules(servers(value)["node"])["memory"]["state"] == "DOWN"
            and delivered(value, 2, 4)
        ),
    )
    site_incident = sources(down)["primary"]["incident_id"]
    metric_incident = rules(servers(down)["node"])["memory"]["incident_id"]
    assert isinstance(site_incident, str) and isinstance(metric_incident, str)
    assert site_incident != metric_incident
    assert sources(down)["secondary"]["state"] == "UP" and sources(down)["secondary"]["fresh"]
    assert servers(down)["node"]["availability"] == "UP" and servers(down)["peer"]["fresh"]
    print("Website and memory incidents reached Telegram and both Mailpit recipients", flush=True)

    stack.helper("telegram", "down")
    receipt = json.loads(stack.helper("event", "notification"))
    delayed = stack.saved_event()
    assert receipt["outcome"] == "notification"
    assert json.loads(stack.helper("event", "duplicate")) == receipt
    isolated = wait_status(
        stack,
        lambda value: (
            value["email"]["accepted"] == 6
            and value["telegram"]["accepted"] == 2
            and value["telegram"]["retrying"] == 1
            and value["telegram"]["in_flight"] == 0
        ),
    )
    assert sources(isolated)["secondary"]["fresh"] and servers(isolated)["peer"]["fresh"]
    assert stack.execute("python", "-m", "sentinel_bot", "healthcheck") == "healthy"
    metadata = startup_metadata(stack)
    starts = http_json(telegram_url + "/api/messages")["startup_count"]
    dependencies = {
        service: stack.inspect(service) for service in SERVICES if service != "sentinel"
    }
    stack.compose("stop", "--timeout", "15", "sentinel")
    restored = json.loads(stack.compose("run", "--rm", "--no-deps", "sentinel", "status"))
    assert sources(restored)["primary"]["fresh"] is False
    assert servers(restored)["node"]["fresh"] is False
    assert sources(restored)["primary"]["incident_id"] == site_incident
    assert rules(servers(restored)["node"])["memory"]["incident_id"] == metric_incident
    assert restored["telegram"]["accepted"] == 2 and restored["email"]["accepted"] == 6
    assert restored["telegram"]["pending"] == 1
    # Dependencies hold volatile inboxes, event state and assigned host ports.
    stack.compose(
        "up", "-d", "--no-deps", "--no-recreate", "--wait", "--wait-timeout", "90", "sentinel"
    )
    for service, before in dependencies.items():
        after = stack.inspect(service)
        assert after["Id"] == before["Id"], f"{service} was recreated during Sentinel restart"
        assert after["State"]["StartedAt"] == before["State"]["StartedAt"], (
            f"{service} was restarted during Sentinel restart"
        )
        assert after["NetworkSettings"]["Ports"] == before["NetworkSettings"]["Ports"], (
            f"{service} published ports changed during Sentinel restart"
        )
    assert stack.inbox_url("telegram-fixture", 8081) == telegram_url
    assert stack.inbox_url("mailpit", 8025) == mail_url
    assert startup_metadata(stack)["count"] == metadata["count"] + 1
    assert http_json(telegram_url + "/api/messages")["startup_count"] == starts + 1
    assert stack.event(delayed) == receipt
    continued = stack.status()
    assert sources(continued)["primary"]["incident_id"] == site_incident
    assert rules(servers(continued)["node"])["memory"]["incident_id"] == metric_incident
    stack.helper("telegram", "up")
    wait_status(stack, lambda value: delivered(value, 3, 6))
    assert http_json(mail_url + "/api/v1/messages")["total"] == 6
    assert http_json(telegram_url + "/api/messages")["failed_attempts"] >= 1
    print(
        "Telegram outage and persisted retry survived restart without accepted resends", flush=True
    )

    stack.compose("pause", "mailpit")
    stack.paused = True
    assert json.loads(stack.helper("event", "notification"))["outcome"] == "notification"
    isolated = wait_status(
        stack,
        lambda value: (
            value["telegram"]["accepted"] == 4
            and value["email"]["accepted"] == 6
            and value["email"]["retrying"] >= 1
        ),
    )
    assert sources(isolated)["secondary"]["fresh"] and servers(isolated)["peer"]["fresh"]
    assert stack.execute("python", "-m", "sentinel_bot", "healthcheck") == "healthy"
    stack.compose("unpause", "mailpit")
    stack.paused = False
    wait_status(stack, lambda value: delivered(value, 4, 8))
    assert http_json(mail_url + "/api/v1/messages")["total"] == 8
    print("SMTP outage left Telegram and polling healthy; queued recipients resumed", flush=True)

    stack.helper("site", "primary", "up")
    stack.helper("metrics", "up")
    wait_status(
        stack,
        lambda value: (
            sources(value)["primary"]["state"] == "UP"
            and rules(servers(value)["node"])["memory"]["state"] == "UP"
            and delivered(value, 6, 12)
        ),
    )
    opening_receipt = json.loads(stack.helper("event", "firing", "--key", "release-occurrence"))
    opening = stack.saved_event()
    assert opening_receipt["outcome"] == "opened"
    assert json.loads(stack.helper("event", "duplicate")) == opening_receipt
    assert stack.event({**opening, "summary": "Changed immutable content."}, expected=409) == {
        "error": {"code": "conflict"}
    }
    wait_status(stack, lambda value: delivered(value, 7, 14))
    assert (
        json.loads(stack.helper("event", "resolved", "--key", "release-occurrence"))["outcome"]
        == "resolved"
    )
    final = wait_status(stack, lambda value: delivered(value, 8, 16))
    assert final["sources"][0]["availability"] == "UNKNOWN"
    assert final["sources"][0]["active_incidents"] == 0
    inbox = http_json(mail_url + "/api/v1/messages")
    assert inbox["total"] == 16
    assert {recipient["Address"] for item in inbox["messages"] for recipient in item["To"]} == {
        "operator@example.test",
        "backup@example.test",
    }
    accepted = http_json(telegram_url + "/api/messages")
    texts = [message["text"] for message in accepted["messages"]]
    assert len(texts) == 8
    assert sum(text.startswith("DOWN:") for text in texts) == 2
    assert sum(text.startswith("RECOVERY:") for text in texts) == 2
    assert [text.split(":", 1)[0] for text in texts[-2:]] == ["FIRING", "RESOLVED"]
    assert sum(site_incident in text for text in texts) == 2
    assert sum(metric_incident in text for text in texts) == 2
    print(
        "Recovery and authenticated push lifecycle preserved order and duplicate receipts",
        flush=True,
    )

    rejected_command = stack.request(
        "telegram-fixture",
        "http://127.0.0.1:8080/command",
        json.dumps({"text": "/status", "user_id": 999999}),
    )
    assert rejected_command["status"] == 202
    assert "update_id" in json.loads(stack.helper("command", "/server node"))
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        accepted = http_json(telegram_url + "/api/messages")
        if accepted["accepted_count"] == 9:
            break
        time.sleep(0.2)
    else:
        raise AssertionError("The authorized cached command did not receive a reply")
    reply = accepted["messages"][-1]["text"]
    assert reply.startswith("Configured servers") and "node" in reply and "peer" not in reply
    assert "metrics-fixture" not in reply and "/dev/synthetic" not in reply
    assert "operator@example.test" not in repr(stack.status())
    stack.compose("restart", "--timeout", "15", "sentinel")
    wait_status(
        stack,
        lambda value: (
            delivered(value, 8, 16)
            and sources(value)["primary"]["fresh"]
            and servers(value)["node"]["cpu_percent"] is not None
        ),
    )
    assert stack.event(opening) == opening_receipt
    time.sleep(3.3)
    assert http_json(telegram_url + "/api/messages")["accepted_count"] == 9
    assert http_json(mail_url + "/api/v1/messages")["total"] == 16
    assert stack.execute("python", "-m", "sentinel_bot", "healthcheck") == "healthy"
    stack.compose("stop", "--timeout", "15", "sentinel")
    assert stack.inspect("sentinel")["State"]["ExitCode"] == 0


def main() -> None:
    started = time.monotonic()
    name = "sentinel-release-smoke-" + uuid4().hex[:10]
    with TemporaryDirectory(prefix="sentinel-release-checkout-") as directory:
        checkout = Path(directory)
        export_checkout(checkout)
        stack = ReleaseStack(checkout, name)
        try:
            stack.compose("config", "--quiet")
            print("Starting the one-command release demo from public files only", flush=True)
            verify_release(stack)
            print(
                "Release smoke passed: clean candidate checkout, isolated website/metrics/push "
                "sources, both sinks, channel outages, durable retries/receipts and incident IDs, "
                "cached authorized commands, restart without accepted resend, rootless/read-only "
                f"and SIGTERM ({time.monotonic() - started:.1f}s)",
                flush=True,
            )
        except BaseException:
            print(stack.compose("logs", "--tail", "30", *SERVICES), flush=True)
            raise
        finally:
            if stack.paused:
                stack.compose("unpause", "mailpit")
            stack.compose("down", "--volumes", "--remove-orphans")
            if stack.command(
                "docker",
                "image",
                "ls",
                "--filter",
                f"reference={stack.image}",
                "--format",
                "{{.ID}}",
            ):
                stack.command("docker", "image", "rm", stack.image)


if __name__ == "__main__":
    main()
