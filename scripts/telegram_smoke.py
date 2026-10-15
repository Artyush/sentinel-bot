"""Exercise the real application and Telegram HTTP boundary in disposable Docker."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from docker_smoke import ROOT, Stack


def main() -> None:
    stack = Stack("sentinel-telegram-smoke-" + uuid4().hex[:10], local=False)
    fixture = ROOT / "scripts" / "fixtures" / "telegram_app_smoke.py"
    with TemporaryDirectory(prefix="sentinel-telegram-smoke-") as directory:
        override = Path(directory) / "isolated.yaml"
        override.write_text("networks:\n  sentinel:\n    internal: true\n", encoding="utf-8")
        stack.base.extend(["-f", str(override)])
        try:
            stack.compose("config", "--quiet")
            print("Building the disposable Telegram verification image", flush=True)
            stack.compose("build", "sentinel", timeout=600)
            print("Image built; verifying Telegram against loopback fixtures", flush=True)
            environ = {
                **os.environ,
                "SENTINEL_CONTAINER_CONFIG": "/app/config.toml",
                "SENTINEL_TELEGRAM_BOT_TOKEN": "",
                "SENTINEL_TELEGRAM_CHAT_ID": "",
                "SENTINEL_TELEGRAM_ALLOWED_USER_IDS": "",
                "SENTINEL_TELEGRAM_TOPIC_ID": "",
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
                    "The offline Telegram application check failed:\n"
                    + result.stdout
                    + result.stderr
                )
            print(result.stdout.strip(), flush=True)
        finally:
            stack.compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
