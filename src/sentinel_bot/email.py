"""An explicitly invoked SMTP check against a local development sink."""

from __future__ import annotations

import math
import os
import smtplib
import socket
import threading
from collections.abc import Mapping
from email.message import EmailMessage


class EmailError(RuntimeError):
    """The local SMTP check was refused, misconfigured, or unsuccessful."""


def send_test_email(environ: Mapping[str, str] | None = None) -> None:
    """Send one synthetic email after explicit local-test opt-in, never on startup."""
    env = os.environ if environ is None else environ
    if env.get("SENTINEL_SMTP_TEST_ENABLED") != "true":
        raise EmailError("Local SMTP test is disabled; enable it explicitly.")
    host = env.get("SENTINEL_SMTP_HOST", "mailpit")
    if host not in {"mailpit", "localhost", "127.0.0.1", "::1"}:
        raise EmailError("SMTP test host must be Mailpit or loopback.")
    try:
        port = int(env.get("SENTINEL_SMTP_PORT", "1025"))
        timeout = float(env.get("SENTINEL_SMTP_TIMEOUT_SECONDS", "5"))
    except ValueError as exc:
        raise EmailError("SMTP test port and timeout must be numbers.") from exc
    if not 1 <= port <= 65535 or not math.isfinite(timeout) or not 0.1 <= timeout <= 10:
        raise EmailError("SMTP test port or timeout is outside the supported range.")

    message = EmailMessage()
    message["From"] = "sentinel@example.test"
    message["To"] = "operator@example.test"
    message["Subject"] = "Sentinel Bot local email test"
    message.set_content("This synthetic message verifies the local Sentinel Bot SMTP sink.\n")

    client = smtplib.SMTP(timeout=timeout, local_hostname="sentinel.local")
    expired = threading.Event()

    def expire() -> None:
        expired.set()
        connection = client.sock
        if connection is not None:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    deadline = threading.Timer(timeout, expire)
    deadline.daemon = True
    deadline.start()
    try:
        code, _ = client.connect(host, port)
        if code != 220:
            raise EmailError("Local SMTP sink rejected the connection.")
        if expired.is_set():
            raise EmailError("Local SMTP test timed out.")
        if client.send_message(message):
            raise EmailError("Local SMTP sink rejected the test recipient.")
        if expired.is_set():
            raise EmailError("Local SMTP test timed out.")
    except (OSError, smtplib.SMTPException) as exc:
        reason = "timed out" if expired.is_set() else "did not accept the test email"
        raise EmailError(f"Local SMTP sink {reason}.") from exc
    finally:
        deadline.cancel()
        client.close()
