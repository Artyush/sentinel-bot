# Running Sentinel Bot

The executable package provides an explicit idle runtime, validated configuration, SQLite startup metadata, local health checks, and a deliberately invoked SMTP test. Website checks, Telegram delivery, event reception, and production email delivery are not implemented. Unsupported configuration sections and live mode fail validation.

Docker Compose runs Python and the development mail sink as separate services. No Telegram token, paid email account, host Python installation, or private infrastructure is required. The first image build needs network access to the pinned public images and build dependencies.

## Local Docker setup

From the project root, run:

```bash
docker compose up -d --build --wait
docker compose ps
docker compose exec sentinel python -m sentinel_bot check-config
docker compose exec sentinel python -m sentinel_bot healthcheck
```

Compose automatically combines `compose.yaml` with `compose.override.yaml`. The `sentinel` service runs Python; `mailpit` captures local SMTP messages. Containers are named `<project>-sentinel` and `<project>-mailpit`, without a replica suffix; the default project gives `sentinel-bot-sentinel` and `sentinel-bot-mailpit`. Open [Mailpit](http://127.0.0.1:8025) to view the inbox. Its SMTP port is available to the application at `mailpit:1025` and is not published on the host.

Send one synthetic test message explicitly:

```bash
docker compose exec sentinel python -m sentinel_bot test-email
```

The message goes from `sentinel@example.test` to `operator@example.test`. Startup, configuration validation, and health checks never send mail. The test command allows only the Mailpit service name or loopback, requires the local test flag, and cannot configure arbitrary recipients or a real provider. Its success reports SMTP acceptance, not internet inbox delivery.

Mailpit is a maintained development mail catcher with the same SMTP/UI purpose as MailHog. Relay is not configured, accepted recipients are restricted to `example.test`, and automatic version checks are disabled. The application uses only the internal Compose network. Mailpit also has a separate bridge for its loopback web UI; it therefore has network connectivity beyond that internal network. The test workflow uses only local SMTP and HTTP.

If port 8025 is already in use:

```bash
MAILPIT_HTTP_PORT=18025 docker compose up -d --wait
```

Then open `http://127.0.0.1:18025`. Alternatively, copy `.env.example` to `.env` and set `MAILPIT_HTTP_PORT`; no secrets are needed. Mailpit keeps at most 100 messages, expires them after 24 hours, and stores its database in a bounded temporary filesystem. Recreating its container clears the development inbox.

Stop the local stack with `docker compose down`. The named `sentinel-state` volume persists. `docker compose down --volumes` also deletes that project's startup metadata and should be used only when intentionally resetting local state.

## Server Docker setup

Use the base file explicitly so the local mail override is not loaded:

```bash
docker compose -f compose.yaml up -d --build --wait
docker compose -f compose.yaml ps
docker compose -f compose.yaml exec sentinel python -m sentinel_bot healthcheck
```

This starts the same application image with its own state volume and no Mailpit, published port, or SMTP test flag. Use a different Compose project name if running local and server configurations on the same Docker host, for example `docker compose -p sentinel-server -f compose.yaml up -d --build --wait`; use that same prefix for later commands.

The server configuration currently runs explicit idle mode. It does not deliver production alerts or run an SMTP server. A production email provider is a separate external service; adding its adapter and verified credentials is a separate implementation task. Do not put the development mail catcher on a public server as a production mail relay.

The configuration is baked into the image from `config/idle.toml`. Change that file and rebuild to adjust supported runtime settings. State is stored at `/var/lib/sentinel` on a dedicated volume. The container runs as UID/GID 10001, uses a read-only root filesystem, drops Linux capabilities, has bounded resources/logs, and has no host filesystem mounts or Docker socket.

Only one process can own a state directory. A second `run` against the same volume fails without altering the first instance's state. SIGTERM stops the process within the Compose shutdown window. SQLite persists startup count and UTC first/last-start times; health uses the running instance's monotonic heartbeat. A stopped process or stale heartbeat is unhealthy. Docker reports an unhealthy container but does not automatically restart it solely because of that health status.

## Native development and checks

Native execution requires Linux/POSIX, Python 3.14, and usable `venv`/`pip`. Docker runs the supported Linux runtime on other Docker-capable hosts. The runtime has no third-party Python dependencies. Build and development dependencies are hash-locked separately.

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements-dev.lock
.venv/bin/python -m pip install --no-deps --no-build-isolation -e .
.venv/bin/python -m sentinel_bot check-config
.venv/bin/python -m sentinel_bot run
```

The default native configuration is `config/idle.toml`, with state under `data/`. Use `SENTINEL_CONFIG` or `--config` before the subcommand for another file; `SENTINEL_STATE_DIR` overrides the state path. Native checks make no external requests. Local SMTP fixtures bind loopback sockets during tests.

```bash
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/mypy src
.venv/bin/python -m pytest --cov=sentinel_bot --cov-report=term-missing
python3 scripts/docker_smoke.py
```

The Docker smoke script creates uniquely named disposable local and server stacks, uses an automatically assigned loopback web port, checks actual container restrictions, startup/restart persistence, invalid config, competing instances, health and SIGTERM, and verifies a test message through Mailpit's HTTP API. It removes only those temporary stacks and their volumes. It does not deploy a server or contact Telegram or an email provider.

CI uses the locked development tools and the same Docker smoke script. Workflow creation does not mean GitHub Actions has run successfully.
