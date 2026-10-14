from __future__ import annotations

import email.policy
import socketserver
import threading
import time
from collections.abc import Iterator
from email.message import EmailMessage
from email.parser import BytesParser

import pytest

from sentinel_bot.email import EmailError, send_test_email


class SMTPFixture(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), SMTPHandler)
        self.messages: list[bytes] = []
        self.greeting_code = 220
        self.reject_recipient = False
        self.greeting_gate: threading.Event | None = None

    @property
    def environment(self) -> dict[str, str]:
        return {
            "SENTINEL_SMTP_TEST_ENABLED": "true",
            "SENTINEL_SMTP_HOST": "127.0.0.1",
            "SENTINEL_SMTP_PORT": str(self.server_address[1]),
        }


class SMTPHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        self.connection.settimeout(5)
        server = self.server
        assert isinstance(server, SMTPFixture)
        if server.greeting_gate is not None:
            server.greeting_gate.wait(timeout=2)
        self.wfile.write(f"{server.greeting_code} synthetic SMTP fixture\r\n".encode("ascii"))
        while command := self.rfile.readline(4096):
            verb = command.split(b" ", 1)[0].strip().upper()
            if verb in (b"EHLO", b"HELO"):
                self.wfile.write(b"250 synthetic fixture\r\n")
            elif verb == b"MAIL":
                self.wfile.write(b"250 sender accepted\r\n")
            elif verb == b"RCPT":
                reply = b"550 recipient rejected\r\n" if server.reject_recipient else b"250 ok\r\n"
                self.wfile.write(reply)
            elif verb == b"DATA":
                self.wfile.write(b"354 enter message\r\n")
                chunks = []
                while (line := self.rfile.readline(16384)) != b".\r\n":
                    if not line:
                        return
                    chunks.append(line)
                server.messages.append(b"".join(chunks))
                self.wfile.write(b"250 message accepted\r\n")
            elif verb == b"QUIT":
                self.wfile.write(b"221 goodbye\r\n")
                return
            elif verb == b"RSET":
                self.wfile.write(b"250 reset\r\n")
            else:
                self.wfile.write(b"502 unsupported command\r\n")


@pytest.fixture
def smtp_server() -> Iterator[SMTPFixture]:
    with SMTPFixture() as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server
        finally:
            server.shutdown()
            thread.join(timeout=5)


def parse_message(raw: bytes) -> EmailMessage:
    result = BytesParser(policy=email.policy.default).parsebytes(raw)
    assert isinstance(result, EmailMessage)
    return result


def test_explicit_email_command_sends_only_synthetic_message(smtp_server: SMTPFixture) -> None:
    send_test_email(smtp_server.environment)

    assert len(smtp_server.messages) == 1
    message = parse_message(smtp_server.messages[0])
    assert message["From"] == "sentinel@example.test"
    assert message["To"] == "operator@example.test"
    assert message["Subject"] == "Sentinel Bot local email test"
    assert message.get_content_type() == "text/plain"
    assert message.get_content().strip()
    assert len(smtp_server.messages[0]) < 16384


@pytest.mark.parametrize("opt_in", [None, "false", "TRUE", "1", "yes"])
def test_email_requires_explicit_opt_in(
    monkeypatch: pytest.MonkeyPatch, opt_in: str | None
) -> None:
    def forbidden_connection(*args: object, **kwargs: object) -> None:
        pytest.fail("an SMTP check must not connect without explicit opt-in")

    monkeypatch.setattr("socket.create_connection", forbidden_connection)
    environment = {"SENTINEL_SMTP_HOST": "127.0.0.1"}
    if opt_in is not None:
        environment["SENTINEL_SMTP_TEST_ENABLED"] = opt_in

    with pytest.raises(EmailError):
        send_test_email(environment)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("SENTINEL_SMTP_HOST", "smtp.example.test"),
        ("SENTINEL_SMTP_HOST", "mailpit.example.test"),
        ("SENTINEL_SMTP_HOST", "127.0.0.1.attacker.test"),
        ("SENTINEL_SMTP_HOST", "127.0.0.2"),
        ("SENTINEL_SMTP_HOST", "https://mailpit"),
        ("SENTINEL_SMTP_HOST", "localhost\r\nInjected: true"),
        ("SENTINEL_SMTP_HOST", ""),
        ("SENTINEL_SMTP_PORT", "0"),
        ("SENTINEL_SMTP_PORT", "65536"),
        ("SENTINEL_SMTP_PORT", "1.5"),
        ("SENTINEL_SMTP_TIMEOUT_SECONDS", "0"),
        ("SENTINEL_SMTP_TIMEOUT_SECONDS", "11"),
        ("SENTINEL_SMTP_TIMEOUT_SECONDS", "nan"),
        ("SENTINEL_SMTP_TIMEOUT_SECONDS", "inf"),
    ],
)
def test_unsafe_or_unbounded_email_settings_are_rejected_before_connecting(
    monkeypatch: pytest.MonkeyPatch, key: str, value: str
) -> None:
    def forbidden_connection(*args: object, **kwargs: object) -> None:
        pytest.fail("invalid SMTP settings must be rejected before connecting")

    monkeypatch.setattr("socket.create_connection", forbidden_connection)
    environment = {
        "SENTINEL_SMTP_TEST_ENABLED": "true",
        "SENTINEL_SMTP_HOST": "127.0.0.1",
        key: value,
    }

    with pytest.raises(EmailError):
        send_test_email(environment)


def test_invalid_host_is_not_reflected_in_error() -> None:
    private_value = "synthetic-secret-do-not-print.example.test"

    with pytest.raises(EmailError) as error:
        send_test_email({"SENTINEL_SMTP_TEST_ENABLED": "true", "SENTINEL_SMTP_HOST": private_value})

    assert private_value not in str(error.value)


def test_rejected_recipient_is_not_reported_as_success(smtp_server: SMTPFixture) -> None:
    smtp_server.reject_recipient = True

    with pytest.raises(EmailError):
        send_test_email(smtp_server.environment)

    assert smtp_server.messages == []


def test_refused_smtp_greeting_is_reported_as_failure(smtp_server: SMTPFixture) -> None:
    smtp_server.greeting_code = 421

    with pytest.raises(EmailError):
        send_test_email(smtp_server.environment)

    assert smtp_server.messages == []


def test_smtp_read_timeout_is_bounded(smtp_server: SMTPFixture) -> None:
    smtp_server.greeting_gate = threading.Event()
    environment = smtp_server.environment | {"SENTINEL_SMTP_TIMEOUT_SECONDS": "0.1"}
    started = time.monotonic()
    try:
        with pytest.raises(EmailError):
            send_test_email(environment)
    finally:
        smtp_server.greeting_gate.set()

    assert time.monotonic() - started < 2
    assert smtp_server.messages == []
