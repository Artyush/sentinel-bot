# Running Sentinel Bot

Sentinel runs configured website checks, confirms outages and recoveries, persists notifications in SQLite, and delivers them to one configured Telegram destination. Authorized Telegram commands and CLI status read cached observations. Incoming events, Linux metrics and production email delivery are not implemented. An explicit local SMTP test is a separate development utility.

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

Configuration is baked into the image; rebuild after changing a supported config file. Only `idle.toml`, `sites.example.toml`, `demo.toml` and `telegram.example.toml` enter the image. State lives at `/var/lib/sentinel` on its own volume. UID/GID 10001, read-only rootfs, dropped capabilities and bounded resources/logs remain the defaults. There are no host filesystem mounts, Docker socket or application listener. Real-server deployment and live Telegram delivery need separate verification.

Only one process can own a state directory. Existing schema 1 or 2 upgrades atomically to schema 3; startup metadata and existing incident identities survive. SIGTERM cancels supervised workers and waits for network cleanup and started database operations before releasing ownership, including repeated cancellation. Health checks the current-run heartbeat and enabled website/Telegram workers. A DOWN website or an expected Telegram outage can coexist with a healthy bot. Docker does not restart a container solely because its health status is unhealthy.

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

Changing URL, marker, status range, confirmation counts, body limit or request timeouts resets only the affected source and cancels obsolete, never-attempted records without fabricated recovery. Already attempted records retain their identity and destination because external acceptance may be unknown. Poll/reminder interval changes preserve incidents.

Snapshots, transitions and outbox records commit together. The queue coalesces obsolete, never-attempted outage/recovery pairs and reminders; attempted and in-flight records remain protected. Limits are 80 pending records, 20 in-flight records and bounded recent histories of 200 completed records and 200 incident audit records. Capacity failure rolls back the observation. No reminders accumulate without an accepted opening send. With Telegram disabled, incidents remain queued for the logical operator route; no sender or reminder backlog runs.

## Telegram setup

Telegram is opt-in. `config/telegram.example.toml` enables it with `[telegram] enabled = true` and contains a public placeholder website. Edit the website inventory before live execution. Credentials and private routing settings belong in environment variables, never TOML:

| Variable | Required value |
| --- | --- |
| `SENTINEL_TELEGRAM_BOT_TOKEN` | Token from [BotFather](https://core.telegram.org/bots/tutorial#obtain-your-bot-token) |
| `SENTINEL_TELEGRAM_CHAT_ID` | One numeric private-chat or group ID |
| `SENTINEL_TELEGRAM_ALLOWED_USER_IDS` | Comma-separated unique numeric user IDs, at most 20 |
| `SENTINEL_TELEGRAM_TOPIC_ID` | Optional positive group topic ID; leave empty for the whole chat |

Use an operator-controlled chat. In a private chat, the user must first start the bot; in a group, add the bot and allow it to send messages. Group members can see its ordinary messages, even when they cannot issue commands. Retrieve IDs through your own trusted Telegram tooling; Sentinel does not discover destinations or expose an unauthenticated setup command. Run only one poller per bot token. An existing webhook, invalid token or competing poller produces a visible startup/runtime error; Sentinel never deletes a webhook automatically.

For Docker, copy `.env.example` to the ignored `.env`, fill the Telegram variables and set `SENTINEL_CONTAINER_CONFIG=/app/config/telegram.example.toml`. Use the base Compose network, which permits outbound HTTPS:

```bash
docker compose -f compose.yaml build sentinel
docker compose -f compose.yaml run --rm --no-deps sentinel check-config
docker compose -f compose.yaml up -d --wait
docker compose -f compose.yaml exec sentinel python -m sentinel_bot status
```

`check-config` validates local configuration without contacting Telegram. `run` validates the bot identity and webhook state, starts long polling and delivers eligible queued incidents. There is no automatic startup/test message. The default local Mailpit override has an internal application network and cannot reach Telegram. The isolated website demo rejects live Telegram configuration. To use Telegram without website polling, enable it in `idle.toml` instead and select that file.

Native execution does not load `.env` automatically. Export the same Telegram variables into your shell, then run `.venv/bin/python -m sentinel_bot --config config/telegram.example.toml run`. The production API address is fixed to verified `https://api.telegram.org`; it cannot be overridden through inventory, environment or commands. API errors expose safe categories rather than request URLs, tokens or response bodies.

### Commands and access

`/help`, `/status`, `/sites`, `/servers` and `/server <alias>` read cached state without triggering probes. In a group, `/status@YourBotUsername` also works. Server commands currently report that no server sources are configured. Replies show their UTC snapshot time, confirmed state, latest observation and freshness; restored readings remain stale until checked again.

Authorization requires the configured chat, an allowed human user and, when configured, the exact topic. Other chats/users/topics, anonymous senders, bots, channel posts and edited messages are ignored. Commands cannot change inventory, recipients or credentials, submit URLs, execute shell/SSH commands or manage services.

Commands allow six per minute per user with a burst of two, and 30 per minute overall with a burst of five. Replies are plain text, bounded to 3500 UTF-16 units. Their in-memory queue holds 20 replies, with at most three delivery attempts each; overflow or restart can lose a reply. Repeat the command if needed. This reply queue is separate from durable incident delivery.

### Delivery and recovery

Incident sends and command replies share a FIFO sender with at least 3.1 seconds between request starts. Incidents have at most eight attempts, persisted before each request. Transient failures use exponential retry with bounded jitter, capped at five minutes unless Telegram requires a longer pause. HTTP 429 applies its `Retry-After` to all sends, including command replies. Supported pauses up to seven days are persisted and honored; a larger hint stops with an explicit configuration error instead of retrying early. The chosen spacing is conservative for one group; see [Telegram's rate guidance](https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this).

The sender acknowledges delivery only after Telegram accepts the message. Accepted records are not resent on restart. If the process stops after external acceptance but before the SQLite acknowledgement, retry can duplicate that message; exactly-once external delivery is not guaranteed. Interrupted attempts keep their event ID and destination. Queued alerts include their incident time and may describe a historical state; commands show the current snapshot.

DOWN precedes its matching RECOVERY. A permanent failure or exhausted opening attempt budget visibly blocks dependent recovery/reminders. A permanently failed reminder suppresses further reminders for that incident; recovery can still be delivered if its opening succeeded. Reminders require a fresh, active DOWN and an accepted opening/reminder. Current-process retry, cooldown and reminder deadlines use monotonic time. Restart conservatively repeats the saved retry/pause or full reminder interval, so clock changes cannot cause an early send but may add delay.

Enabling Telegram binds never-attempted logical-operator records to the configured destination. Disabling it pauses pending Telegram delivery. Changing chat/topic fails old pending records with `destination_changed`; it does not forward old history to the new destination. Recovery stays bound to the opening destination; if that route is no longer active after an accepted opening, recovery fails with `destination_changed`. Source changes and flapping do not discard attempted records whose external acceptance is uncertain.

CLI status includes Telegram `enabled`, `pending`, `retrying`, `failed`, `blocked`, `accepted`, `in_flight`, `paused` and a normalized `last_error`. Counts describe bounded retained records, not an all-time archive. API outages and rate pauses do not stop website checks. Invalid credentials, webhook conflicts and unexpected worker errors stop visibly. The polling checkpoint is durable but expires after 24 hours to handle Telegram's bounded update retention and changing IDs after a quiet period; command replies are still best-effort. See the [Bot API update contract](https://core.telegram.org/bots/api#getting-updates).

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
python3 scripts/telegram_smoke.py
```

Tests use local HTTP/SMTP fixtures and temporary SQLite, never real monitored targets or providers. Foundation smoke checks disposable local/server stacks, actual container restrictions, restart, ownership, health, SIGTERM and one explicit Mailpit message. Monitoring smoke verifies independent sources, outage/recovery, atomic queue, stale restart and incident identity. Telegram smoke runs the real application and HTTP adapter against loopback fixtures inside an isolated container: access checks, cached commands, 429/retry, ordered outage/recovery and restart continuity. It needs no bot token and sends no real Telegram messages. Each script creates unique temporary projects, clears inherited Telegram credentials, and removes only its own containers/volumes. CI runs the same checks; local success does not establish a successful GitHub Actions run or real deployment.
