# Running Sentinel Bot

Sentinel runs configured website checks, confirms outages and recoveries, persists incidents and pending notifications in SQLite, and provides cached CLI status. Telegram delivery, incoming events, Linux metrics and production email delivery are not implemented. The outbox currently stores events for a logical operator route; no alert sender runs. An explicit local SMTP test is a separate development utility.

Docker runs Python and the optional Mailpit development sink as separate services. Local checks need no Telegram token, paid email account or private infrastructure. The first image build needs network access to pinned public images and dependencies.

## Local Docker setup

From the project root:

```bash
docker compose up -d --build --wait
docker compose ps
docker compose exec sentinel python -m sentinel_bot check-config
docker compose exec sentinel python -m sentinel_bot healthcheck
docker compose exec sentinel python -m sentinel_bot status
```

The default configuration is explicit idle mode. Compose combines `compose.yaml` and `compose.override.yaml`: Python owns the state volume; Mailpit captures local SMTP. Containers are named `<project>-sentinel` and `<project>-mailpit`, without a replica suffix. Open [Mailpit](http://127.0.0.1:8025) to view its inbox. SMTP is available at `mailpit:1025` inside Compose and is not published on the host.

Send one synthetic message deliberately:

```bash
docker compose exec sentinel python -m sentinel_bot test-email
```

It goes from `sentinel@example.test` to `operator@example.test`. Startup, checks, status and health never send mail. The test command requires the local test flag, allows only Mailpit or loopback and has fixed synthetic recipients. Success means local SMTP acceptance.

The application uses an internal network in the local override. Mailpit also has a bridge for its loopback UI, which gives the sink outbound connectivity. Relay and automatic version checks are disabled; accepted recipients are restricted to `example.test`. Mailpit retains at most 100 messages for 24 hours in a bounded temporary filesystem. Recreating its container clears the inbox.

For another UI port:

```bash
MAILPIT_HTTP_PORT=18025 docker compose up -d --wait
```

Open `http://127.0.0.1:18025`. You can also copy `.env.example` to `.env` and set `MAILPIT_HTTP_PORT`. `docker compose down` preserves the named state volume; `down --volumes` deletes that project's state and should be used only for an intentional reset.

## Synthetic website monitoring

A separate override runs two local website fixtures without published ports, Mailpit or external requests:

```bash
docker compose -p sentinel-demo -f compose.yaml -f compose.monitoring.yaml up -d --build --wait
docker compose -p sentinel-demo -f compose.yaml -f compose.monitoring.yaml exec sentinel python -m sentinel_bot status
```

Both sources initially become UP without a recovery event. To simulate an outage:

```bash
docker compose -p sentinel-demo -f compose.yaml -f compose.monitoring.yaml exec site-fixture python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/control/primary', data=b'down', timeout=2).close()"
```

After three failures, `primary` becomes DOWN and one notification is pending. Repeat the command with `data=b'up'` to recover after two successes, or `data=b'slow'` to exercise timeouts while `secondary` continues polling. Read status again to see confirmed state, observation, freshness and queue count. Restarting Sentinel retains the active incident and does not create another logical DOWN. Restored readings remain stale until a new check completes.

Stop this stack with the same Compose prefix followed by `down`. Its state is separate from the default local stack. This fixture covers website monitoring and queued incidents; it does not deliver alerts.

## Configured websites and server Docker setup

Use the base Compose file explicitly for outbound website monitoring. The automatic local override has an internal application network and cannot reach public websites.

Edit `config/sites.example.toml` with your intended read-only public targets, then build and select that baked configuration:

```bash
SENTINEL_CONTAINER_CONFIG=/app/config/sites.example.toml docker compose -f compose.yaml up -d --build --wait
docker compose -f compose.yaml exec sentinel python -m sentinel_bot status
docker compose -f compose.yaml exec sentinel python -m sentinel_bot healthcheck
```

The example contains only a public placeholder. Configuration validation does not probe it; `run` probes enabled configured targets. Use the same `SENTINEL_CONTAINER_CONFIG` value for subsequent `up` commands, or set it in `.env`. For idle server execution, omit the variable. Use a different project name when running several stacks on one host.

Configuration is baked into the image; rebuild after changing a supported config file. Only the three explicit example configurations enter the image. State lives at `/var/lib/sentinel` on its own volume. UID/GID 10001, read-only rootfs, dropped capabilities and bounded resources/logs remain the defaults. There are no host filesystem mounts, Docker socket or application listener. Real-server deployment and notification delivery need separate verification.

Only one process can own a state directory. Schema 1 startup metadata upgrades atomically to schema 2; startup count and first-start time survive. SIGTERM cancels supervised workers and waits for started database operations before releasing ownership. Health checks current-run heartbeat and each source worker's progress. A DOWN website can coexist with a healthy bot. Docker does not restart a container solely because its health status is unhealthy.

## Website configuration and incident behavior

Native examples use `[runtime] mode = "monitor"` and one or more `[[sites]]` tables. `alias` and `url` are required. Optional settings and defaults:

| Setting | Default |
| --- | --- |
| `marker` | No body read; optional exact UTF-8 text |
| `status_min` / `status_max` | 200 / 399 |
| `poll_interval_seconds` | 30 |
| `connect_timeout_seconds` / `total_timeout_seconds` | 5 / 10 |
| `failure_confirmation` / `recovery_confirmation` | 3 / 2 |
| `reminder_interval_seconds` | 1800 after an accepted opening/reminder |
| `max_body_bytes` | 65536, encoded and decoded |
| `enabled` | true |
| `runtime.max_concurrency` | 5 |

Inventory is bounded to 20 unique aliases; disabled rows are still validated. Live URLs use HTTP(S), ports 80 or 443 and public addresses. Credentials, query strings, fragments and local/private addresses are rejected. Every connection validates all DNS answers; at most three same-origin redirects are allowed. TLS is verified, and ambient proxies/cookies are disabled. Demo mode allows only `http://site-fixture:8080`.

The network deadline covers DNS, redirects and body reads. Shared-slot waiting is separately bounded to one request deadline. Local capacity exhaustion defers the check, clears pending confirmations and does not count as website failure. Each source polls sequentially and skips missed slots.

The first success sets UP without RECOVERY. Confirmed DOWN starts at the first failure in its series; recovery uses the same incident identity. Opposite observations reset pending confirmations. Restart and stale gaps also reset pending series while preserving confirmed incidents. Freshness expires after `2 × poll interval + total request deadline`; missing or restored observations remain stale. Source progress health allows `2 × poll interval + 2 × total request deadline` for slot wait plus probing.

Changing URL, marker, status range, confirmation counts, body limit or request timeouts resets only the affected source and cancels obsolete unsent records without fabricated recovery. Poll/reminder interval changes preserve incidents.

Snapshots, transitions and outbox records commit together. The queue keeps the latest outage/recovery pair per source, protects in-flight records and cancels unsent reminders after recovery. Limits are 80 pending records, 20 in-flight records and 200 records each for completed sends and incident audit. Capacity failure rolls back the observation. No reminders accumulate without an accepted opening send. Delivery acknowledgement and ordering are tested through a fake boundary; no Telegram/email adapter exists yet.

## Native development and checks

Native execution requires Linux/POSIX, Python 3.14 and usable `venv`/`pip`. Docker provides the Linux runtime on other Docker-capable hosts. HTTP uses aiohttp and cancellable aiodns; runtime, build and development dependencies are pinned and hash-locked.

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements-dev.lock
.venv/bin/python -m pip install --no-deps --no-build-isolation -e .
.venv/bin/python -m sentinel_bot check-config
.venv/bin/python -m sentinel_bot --config config/sites.example.toml check-config
.venv/bin/python -m sentinel_bot --config config/sites.example.toml run
```

The default native file is `config/idle.toml`, with state under `data/`. `SENTINEL_CONFIG` or `--config` selects another file; `SENTINEL_STATE_DIR` overrides state. `SENTINEL_CONTAINER_CONFIG` is a separate Compose setting so native paths cannot replace container paths. In another terminal, use the same config and state directory with `status` or `healthcheck`. Status reads existing state and never creates a database or starts a request; before the first run it reports a missing-state error.

```bash
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/mypy src
.venv/bin/python -m pytest --cov=sentinel_bot --cov-report=term-missing
python3 scripts/docker_smoke.py
python3 scripts/monitoring_smoke.py
```

Tests use local HTTP/SMTP fixtures and temporary SQLite, never real monitored targets or providers. Foundation smoke checks disposable local/server stacks, actual container restrictions, restart, ownership, health, SIGTERM and one explicit Mailpit message. Monitoring smoke verifies independent sources, outage/recovery, atomic queue, stale restart and incident identity. Each script creates unique temporary projects and removes only its own containers/volumes. CI runs the same checks; local success does not establish a successful GitHub Actions run or real deployment.
