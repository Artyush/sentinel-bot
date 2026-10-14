"""CLI for configuration, idle runtime, local health, and deliberate SMTP tests."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from sentinel_bot.config import ConfigError, load_settings
from sentinel_bot.email import EmailError, send_test_email
from sentinel_bot.runtime import InstanceError, healthcheck, run
from sentinel_bot.storage import StorageError


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sentinel-bot")
    parser.add_argument(
        "--config", type=Path, default=Path(os.environ.get("SENTINEL_CONFIG", "config/idle.toml"))
    )
    parser.add_argument("command", choices=("check-config", "run", "healthcheck", "test-email"))
    args = parser.parse_args(argv)
    try:
        settings = load_settings(args.config)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    if args.command == "check-config":
        print("Configuration is valid")
        return 0
    if args.command == "healthcheck":
        healthy = healthcheck(settings)
        print("healthy" if healthy else "unhealthy")
        return 0 if healthy else 1
    if args.command == "test-email":
        try:
            send_test_email()
        except EmailError as exc:
            print(f"Email test failed: {exc}", file=sys.stderr)
            return 1
        print("Test email accepted by the local SMTP sink")
        return 0

    try:
        asyncio.run(run(settings))
    except (InstanceError, StorageError) as exc:
        print(f"Runtime error: {exc}", file=sys.stderr)
        return 1
    except OSError:
        print("Runtime error: Local state cannot be accessed.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
