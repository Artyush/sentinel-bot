"""Trigger fixed synthetic sources from inside the release demo fixture."""

import argparse
import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener
from uuid import uuid4

from sentinel_bot.demo import DEMO_SOURCE_TOKEN

EVENT_FILE = Path("/tmp/sentinel-demo-event.json")
MAX_BYTES = 4 * 1024 * 1024


def request(url: str, data: bytes | None = None, *, headers: dict[str, str] | None = None) -> bytes:
    """Contact one fixed demo route with no ambient proxy and bounded I/O."""
    with build_opener(ProxyHandler({})).open(
        Request(url, data=data, headers=headers or {}), timeout=5
    ) as response:
        result = response.read(MAX_BYTES + 1)
        if len(result) > MAX_BYTES:
            raise ValueError("Demo response exceeded its size limit.")
        return result


def event_payload(kind: str, key: str, path: Path) -> bytes:
    """Save the immutable payload before sending, so duplicate retries keep its identity."""
    if kind == "duplicate":
        with path.open("rb") as source:
            result = source.read(16385)
        if len(result) > 16384:
            raise ValueError("Saved demo event exceeded its size limit.")
        json.loads(result)
        return result
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", key) is None:
        raise ValueError("Demo incident key must be a bounded identifier.")
    payload = {
        "event_id": str(uuid4()),
        "occurred_at": datetime.now(UTC).isoformat(),
        "kind": kind,
        "type": "service.health",
        "severity": "info" if kind == "resolved" else "warning",
        "summary": f"Synthetic service {kind}.",
    }
    if kind != "notification":
        payload["incident_key"] = key
    result = json.dumps(payload).encode()
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(result)
    os.replace(temporary, path)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    site = actions.add_parser("site", help="Change a synthetic website")
    site.add_argument("alias", choices=("primary", "secondary"))
    site.add_argument("state", choices=("up", "down", "slow"))
    metrics = actions.add_parser("metrics", help="Change the primary synthetic exporter")
    metrics.add_argument(
        "state", choices=("up", "memory-high", "high", "between", "missing", "down")
    )
    telegram = actions.add_parser("telegram", help="Fail or restore synthetic Telegram delivery")
    telegram.add_argument("state", choices=("up", "down"))
    command = actions.add_parser("command", help="Submit a synthetic authorized cached command")
    command.add_argument("text", choices=("/help", "/status", "/sites", "/servers", "/server node"))
    event = actions.add_parser("event", help="Submit or retry an authenticated synthetic event")
    event.add_argument("kind", choices=("notification", "firing", "resolved", "duplicate"))
    event.add_argument("--key", default="demo-incident")
    actions.add_parser("inbox", help="Read synthetic Telegram acceptance history")
    args = parser.parse_args(argv)
    try:
        match args.action:
            case "site":
                result = request(
                    f"http://site-fixture:8080/control/{args.alias}", args.state.encode()
                )
            case "metrics":
                result = request("http://metrics-fixture:9100/control", args.state.encode())
            case "telegram":
                result = request("http://127.0.0.1:8080/control", args.state.encode())
            case "command":
                result = request(
                    "http://127.0.0.1:8080/command",
                    json.dumps({"text": args.text}).encode(),
                    headers={"Content-Type": "application/json"},
                )
            case "event":
                result = request(
                    "http://sentinel:8080/api/v1/events",
                    event_payload(args.kind, args.key, EVENT_FILE),
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {DEMO_SOURCE_TOKEN}",
                    },
                )
            case "inbox":
                result = request("http://127.0.0.1:8081/api/messages")
            case _:
                raise AssertionError("Unknown demo action")
        print(result.decode())
    except (OSError, ValueError, HTTPError, URLError) as error:
        category = f"HTTP {error.code}" if isinstance(error, HTTPError) else "local request failed"
        print(
            f"Demo action failed: {category}. Retain event and retry duplicate if needed.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
