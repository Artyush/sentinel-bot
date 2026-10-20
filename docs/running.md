# Running Sentinel Bot

This guide covers the implemented application. For every supported TOML key, environment variable, default and validation limit, see the [configuration reference](configuration.md). The [website configuration](../config/sites.example.toml) and [Telegram configuration](../config/telegram.example.toml) share a separate [endpoint inventory](../config/checks.example.toml), with commented optional settings and disabled additional probes.

## Contents

- [Available features](#available-features)
- [Choose a configuration](#choose-a-configuration)
- [Local Docker and Mailpit](#local-docker-and-mailpit)
- [Offline website demo](#offline-website-demo)
- [Monitor public websites](#monitor-public-websites)
- [Configure endpoint inventory](#configure-endpoint-inventory)
- [Configure Telegram](#configure-telegram)
- [Receive events from servers](#receive-events-from-servers)
- [Check content with marker](#check-content-with-marker)
- [Verify a notification](#verify-a-notification)
- [Read status, logs and incident history](#read-status-logs-and-incident-history)
- [Apply changes and stop a stack](#apply-changes-and-stop-a-stack)
- [Troubleshooting](#troubleshooting)
- [Native development and checks](#native-development-and-checks)

## Available features

| Capability | Current behavior |
| --- | --- |
| Website monitoring | Configured HTTP(S) GET/POST requests, independent endpoint intervals, optional JSON request bodies, accepted status range, content markers, latency and normalized failure reasons |
| Incident policy | UNKNOWN/UP/DOWN, consecutive failure/recovery confirmation, stable incident IDs and fresh-only reminders after accepted delivery |
| Telegram | One fixed private chat, group/topic, or channel for alerts; cached authorized commands in private chats/groups |
| Persistent delivery | SQLite outbox, bounded retries, shared send limits, provider rate pauses and restart continuity |
| Incoming events | Source-authenticated POST, durable receipts, duplicate suppression and occurrence-based firing/resolution |
| Operator tools | Local configuration validation, cached JSON status and worker health; commands do not trigger website probes |
| Local email testing | Separate Mailpit container, browser inbox and an explicitly invoked synthetic SMTP message |
| Docker | Non-root application, read-only root filesystem, dedicated state volume and isolated synthetic checks |

Linux/Node Exporter metrics and production email alerts are not implemented. `/servers` and `/server <alias>` currently report that no server metrics sources are configured. Mailpit's test message is a local utility; incidents are delivered through Telegram only. The [incoming events API](api.md) documents requests, authentication, responses and sender retry rules.

## Choose a configuration

Run commands from the cloned repository root. Docker requires a running engine and the Compose plugin (`docker compose`). The first build needs access to pinned public images and dependencies; native development requires Linux/POSIX and Python 3.14.

| File | Purpose | Network and delivery |
| --- | --- | --- |
| [`config/idle.toml`](../config/idle.toml) | Run process ownership, state and health without website checks | No website probes; Telegram disabled by default |
| [`config/sites.example.toml`](../config/sites.example.toml) | Monitor operator-selected public websites | Outbound HTTP(S); Telegram disabled |
| [`config/telegram.example.toml`](../config/telegram.example.toml) | Website monitoring and Telegram | Outbound HTTP(S), Telegram credentials required |
| [`config/demo.toml`](../config/demo.toml) | Fast synthetic website checks | Named local fixture only; live Telegram prohibited |
| [`config/ingress.example.toml`](../config/ingress.example.toml) | Push-only source and Telegram delivery | Optional private listener; HTTPS proxy provisioned separately |
| [`config/checks.example.toml`](../config/checks.example.toml) | Shared endpoint inventory for website/Telegram configurations | Endpoint tables only; do not select it as the main configuration |

The five main configurations and shared endpoint inventory are baked into the image. Edit a supported file and rebuild; creating another host TOML file does not automatically put it in the image. Native execution can select another main file with `--config`. Main files support `[runtime]`, `[telegram]`, optional `[ingress]`/`[[sources]]` and inline `[[sites]]`, or `runtime.sites_file` instead of inline sites. The separate endpoint file supports only `[[sites]]`; adding guessed `[email]` or `[[servers]]` tables fails validation.

If `.env` does not exist, copy the template once:

```bash
cp .env.example .env
```

Keep an existing `.env` and edit it instead of overwriting it. Compose loads it automatically; the native Python application does not. Exported shell variables can override Compose's `.env` values; see [Docker's interpolation rules](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/).

Use a consistent project name (`-p`) for every command targeting a stack. The examples use separate local, live and demo projects so their state and network settings stay independent. Copy commands only, without shell prompts, command output or JSON results.

## Local Docker and Mailpit

Start an idle stack with the local email sink. The explicit configuration selection keeps this setup idle even when `.env` selects live monitoring:

```bash
SENTINEL_CONTAINER_CONFIG=/app/config/idle.toml docker compose -p sentinel-local up -d --build --wait
docker compose -p sentinel-local ps
docker compose -p sentinel-local exec sentinel python -m sentinel_bot check-config
docker compose -p sentinel-local exec sentinel python -m sentinel_bot healthcheck
docker compose -p sentinel-local exec sentinel python -m sentinel_bot status
```

Compose automatically combines `compose.yaml` with `compose.override.yaml`. Sentinel owns its state volume; Mailpit runs separately. Container names are `<project>-sentinel` and `<project>-mailpit`, without a replica suffix. See [Docker's Compose file selection rules](https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/).

Open [Mailpit](http://127.0.0.1:8025). SMTP is `mailpit:1025` inside Compose and is not published to the host. To send one synthetic message deliberately:

```bash
docker compose -p sentinel-local exec sentinel python -m sentinel_bot test-email
```

The fixed sender and recipient are `sentinel@example.test` and `operator@example.test`. SMTP acceptance is not a production inbox-delivery guarantee. Startup, status and health send no mail. Relay/version checks are disabled; accepted recipients are limited to `example.test`. Mailpit keeps at most 100 messages for 24 hours in temporary storage; recreating its container clears its inbox.

For a different UI port, set `MAILPIT_HTTP_PORT=18025` in `.env` and rerun the startup command. The application network in this local override is internal and cannot reach public websites or Telegram. Mailpit also has a separate bridge for its loopback UI.

## Offline website demo

Run two synthetic websites without credentials, Mailpit, published ports or external requests:

```bash
docker compose -p sentinel-demo -f compose.yaml -f compose.monitoring.yaml up -d --build --wait
docker compose -p sentinel-demo -f compose.yaml -f compose.monitoring.yaml exec sentinel python -m sentinel_bot status
```

Both sources initially become UP without a recovery event. Simulate a primary outage:

```bash
docker compose -p sentinel-demo -f compose.yaml -f compose.monitoring.yaml exec site-fixture python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/control/primary', data=b'down', timeout=2).close()"
```

After three failed checks, `primary` becomes DOWN with a pending notification. Repeat with `data=b'up'` to recover after two successes, or `data=b'slow'` to exercise timeouts while `secondary` keeps polling. Restarting Sentinel preserves the incident ID; restored readings remain stale until checked again. This demo queues incidents without delivering alerts to Telegram.

Stop the demo and preserve its state:

```bash
docker compose -p sentinel-demo -f compose.yaml -f compose.monitoring.yaml down
```

## Monitor public websites

Edit `config/checks.example.toml`: replace the enabled public placeholder with a repeatedly safe endpoint you intend to monitor. It is loaded by `config/sites.example.toml` through `runtime.sites_file`. Add a separate `[[sites]]` table for each endpoint, using a unique alias; at most 20 entries are supported, including disabled examples. Full settings and ranges are in [website configuration](configuration.md#website-checks-sites).

Set this line in `.env`:

```dotenv
SENTINEL_CONTAINER_CONFIG=/app/config/sites.example.toml
```

Build, validate locally, and start a dedicated outbound stack:

```bash
docker compose -p sentinel-live -f compose.yaml build sentinel
docker compose -p sentinel-live -f compose.yaml run --rm --no-deps sentinel check-config
docker compose -p sentinel-live -f compose.yaml up -d --wait
docker compose -p sentinel-live -f compose.yaml exec sentinel python -m sentinel_bot status
```

Use `-f compose.yaml` explicitly. It excludes the automatic local override and permits outbound requests. Keep a different project name from an existing internal-network local stack; changing file selection does not safely migrate an already-created network.

Live targets must use HTTP(S), ports 80 or 443 and public addresses. URLs cannot include credentials, query strings or fragments. TLS and every DNS answer are checked. GET follows at most three same-origin redirects; POST rejects redirects and never replays the body to a redirect target. Ambient proxies/cookies are disabled. `localhost`, private IPs and local Docker service names are rejected outside the restricted demo. There is no browser/JavaScript execution, authentication-header setting, SSH, service restart or arbitrary target discovery.

A source never overlaps its own checks. Slow sources are independent; missed polling slots are skipped. Waiting for shared capacity is bounded and can defer a check without counting as website failure. Freshness and worker progress are separate from the confirmed incident state.

## Configure endpoint inventory

Keep process/Telegram settings in `config/sites.example.toml` or `config/telegram.example.toml`. Both select the same file through:

```toml
[runtime]
mode = "monitor"
sites_file = "checks.example.toml"
```

`sites_file` is resolved relative to the main TOML file, so the supplied Docker image loads `/app/config/checks.example.toml`. It must stay within that directory; absolute paths, parent traversal and escaping symlinks are rejected. Do not also add inline `[[sites]]` to a main file using this setting. Older main files with inline sites remain supported when `sites_file` is absent.

In `config/checks.example.toml`, repeat `[[sites]]` for independent endpoints on the same or different services:

```toml
[[sites]]
alias = "api_health"
url = "https://example.com/health"
method = "GET"
poll_interval_seconds = 15
enabled = false

[[sites]]
alias = "api_probe"
url = "https://example.com/health/probe"
method = "POST"
json_body = { probe = "availability" }
poll_interval_seconds = 60
enabled = false
```

These rows already exist as disabled examples. Replace their URLs and explicitly enable intended checks; do not append duplicate aliases. Each endpoint has its own observation, confirmation counters, incident and polling interval. The default method is GET. POST accepts an optional JSON object up to 4096 encoded UTF-8 bytes; omit `json_body` for a bodyless POST. A GET cannot contain `json_body`.

Choose a POST endpoint designed for repeated health probes, including repeated requests after a timeout with an unknown outcome. Do not use business actions such as creating payments or users. Sentinel does no login, multi-step workflow or immediate transport retry; each attempt belongs to the endpoint's polling schedule. POST 301/302/303/307/308 responses fail as `redirect` without following them. See [POST body encoding and limits](configuration.md#post-health-probes).

Only the original GET `example` endpoint is enabled in the shared sample. Its marker is commented out; disabled example rows are still validated. Editing the shared inventory affects both main configurations when rebuilt/restarted. The demo continues using its own inline synthetic fixture inventory.

## Configure Telegram

### Select the bot and destination

Create a bot through [BotFather](https://core.telegram.org/bots/tutorial#obtain-your-bot-token). Choose one destination:

| Destination | Setup | Commands |
| --- | --- | --- |
| Private chat | The allowed human user starts the bot first | Supported in this configured chat |
| Group/supergroup | Add the bot and allow sending; optionally select one exact topic | Supported for allowed human users |
| Broadcast channel | Add the bot as an administrator with permission to publish messages | Outgoing alerts only; channel posts are ignored |

An optional topic is for groups, not broadcast channels. With a channel selected, private messages to that bot do not bypass the configured-chat restriction. Group/channel members can see its ordinary alerts even when they cannot issue commands.

Use the full signed numeric Bot API chat ID. A channel's positive internal ID is not its Bot API ID. For a private channel message link such as `https://t.me/c/1234567890/7`, convert the number after `/c/` using `-(1000000000000 + internal_id)`: the synthetic example becomes `-1001234567890`. The trailing `7` is the message ID. See [Telegram's message-link format](https://core.telegram.org/api/links#message-links) and [Bot API ID conversion](https://core.telegram.org/api/bots/ids#supergroupchannel-ids).

For a public channel, `getChat` accepts `@channelusername` and returns `result.id`. For example, create a GET request in Postman to `https://api.telegram.org/bot{{bot_token}}/getChat`, define `bot_token` locally and set query parameter `chat_id` to the channel username. `getMe` verifies which bot owns a token. Use the returned numeric ID in Sentinel; keep actual tokens and private responses out of shared screenshots. See [getChat](https://core.telegram.org/bots/api#getchat).

To obtain your own human user ID, do this **before starting Sentinel's poller**, with no other poller or webhook using that token. Send `/start` to your bot in a private chat. In Postman, GET `https://api.telegram.org/bot{{bot_token}}/getUpdates` with query parameters `timeout=0` and `allowed_updates=["message"]`. Find your message in `result` and use its `message.from.id` for the allowlist. Its `message.chat.id` selects that private chat if you want private-chat delivery instead of a group/channel. Do not confuse a group/channel ID with a human user's ID. This is Telegram's [getUpdates method](https://core.telegram.org/bots/api#getupdates), not a Sentinel command.

### Enable Telegram and start

Edit `config/checks.example.toml` with your intended endpoints; `config/telegram.example.toml` loads that inventory and enables Telegram. In `.env`, select the main Telegram config and fill the four channel variables:

```dotenv
SENTINEL_CONTAINER_CONFIG=/app/config/telegram.example.toml
SENTINEL_TELEGRAM_BOT_TOKEN=REPLACE_WITH_BOTFATHER_TOKEN
SENTINEL_TELEGRAM_CHAT_ID=-1001234567890
SENTINEL_TELEGRAM_ALLOWED_USER_IDS=123456789
SENTINEL_TELEGRAM_TOPIC_ID=
```

The token and IDs above are placeholders, not working credentials. The human-user allowlist is required even for a channel-only destination; it contains user IDs, not the channel ID. Leave topic empty unless using a group topic. Adding credentials alone does not enable Telegram: the selected TOML must contain `[telegram] enabled = true`. See [Telegram settings and ID constraints](configuration.md#telegram).

Run the same four `sentinel-live` commands from [website startup](#monitor-public-websites). They select the Telegram example through `.env`. If you also run another stack, only one poller may use the same token. To run Telegram without website checks, enable `[telegram]` in `idle.toml` and select that file.

`check-config` only validates local settings. `run` calls `getMe`/`getWebhookInfo`, starts long polling and delivers eligible queued incidents. It sends no automatic startup/test message. Wrong credentials, an existing webhook or competing poller stop visibly; Sentinel never removes a webhook automatically. Production requests use verified `https://api.telegram.org`; operator settings cannot redirect that API boundary.

### Commands

| Command | Cached response |
| --- | --- |
| `/help` | Supported commands |
| `/status` | Source state, freshness and Telegram delivery counters |
| `/sites` | Website observations and confirmed incidents |
| `/servers` | Currently reports no configured server sources |
| `/server <alias>` | Currently reports no configured server sources; never probes a submitted address |

In a group, use `/status@YourBotUsername` to address the intended bot. Chat, optional topic and allowed-human checks happen before cache access; other users/chats, bots, anonymous senders, channel posts and edited messages are ignored. Authorization cannot change routing or configuration.

Replies are plain text with UTC snapshot time, bounded to 3500 UTF-16 units. Limits are six commands/minute per user (burst two), 30/minute overall (burst five), a 20-reply in-memory queue and up to three attempts. Overflow/restart may lose a reply; repeat the command if needed. Command replies have a different persistence contract from incident delivery.

## Receive events from servers

The [API guide](api.md) and [OpenAPI file](openapi.yaml) describe the single `POST /api/v1/events` route. Configure the source in [`config/ingress.example.toml`](../config/ingress.example.toml); the example accepts `service.health` and `security.attack` from the push-only alias `app`. Set the Telegram credentials from the previous section and a distinct `SENTINEL_SOURCE_APP_TOKEN` in the ignored `.env`. Generate a token locally:

```bash
python3 -c 'import secrets; print(secrets.token_urlsafe(32))'
```

Start a separate outbound stack with the explicit ingress override:

```bash
docker compose -p sentinel-ingress -f compose.yaml -f compose.ingress.yaml build sentinel
docker compose -p sentinel-ingress -f compose.yaml -f compose.ingress.yaml run --rm --no-deps sentinel check-config
docker compose -p sentinel-ingress -f compose.yaml -f compose.ingress.yaml up -d --wait
docker compose -p sentinel-ingress -f compose.yaml -f compose.ingress.yaml exec sentinel python -m sentinel_bot status
```

Only one running stack may poll the same Telegram bot token. Use the same project/file selection to stop an earlier live stack before moving that token here; its state volume remains separate. The ordinary local override is excluded because its network blocks outgoing Telegram requests.

The override exposes container port 8080 to its network, with **no published host port**. Put an operator-managed HTTPS reverse proxy on that network and route only `/api/v1/events` to `sentinel:8080`. The proxy must enforce TLS, connection/header limits and a request-body limit no greater than the application's configured bound. It must preserve the source's Authorization header. The application does not trust forwarded headers as source identity. This repository does not provision a public hostname or TLS certificate.

For native local testing, change `[ingress] host` to `"127.0.0.1"`, export Telegram variables and the source token, then run:

```bash
.venv/bin/python -m sentinel_bot --config config/ingress.example.toml check-config
.venv/bin/python -m sentinel_bot --config config/ingress.example.toml run
```

Use `http://127.0.0.1:8080/api/v1/events` only for local loopback requests; remote senders use HTTPS. Import `docs/openapi.yaml` into Postman, set the actual base URL/source token locally and generate a fresh UUID/UTC timestamp for each new event. Preserve the same request on retry. Importing documentation does not enable a listener or send a message.

An enabled receiver currently requires Telegram and at least one enabled source. Each source's `token_env` names its environment credential. If adding another source in Docker, explicitly forward its token variable in your Compose override as the example does for `SENTINEL_SOURCE_APP_TOKEN`; merely adding a host `.env` key does not pass it into the container. Configuration changes require rebuild/recreation. To rotate a token, replace its environment value and recreate the container; the old token then stops authenticating.

`mode = "push"` defines no target and creates no probe worker. To combine polling and incoming reports, keep an explicit `[[sites]]` entry and use `mode = "hybrid"` in `[[sources]]` with the same alias; their state remains separate. The cached `sources` view and Telegram `/status` show last received/event times and active occurrence counts, with availability UNKNOWN. Removing a source or changing website settings does not discard its already accepted push deliveries.

## Check content with marker

Without `marker`, a website check inspects the HTTP status and closes the response without reading its body. To require content as well, uncomment the example inside that endpoint's `[[sites]]` table in `config/checks.example.toml`:

```toml
marker = "Example Domain"
```

The configured status range must match and the response must contain this exact, case-sensitive UTF-8 substring. This can detect an error or maintenance page returning HTTP 200. It is a text search in the HTTP response, not a CSS selector, regular expression or search in a JavaScript-rendered page. Choose a stable fragment from your intended response.

Disable the content check by removing/commenting the field. **`marker = ""` is invalid.** A marker is at most 4096 UTF-8 bytes and cannot exceed `max_body_bytes`; encoded and decoded response bodies are each bounded to that body's configured limit, at most 65536 bytes. A missing marker or body-limit violation is a failed observation.

Changing marker, URL, method, JSON body, accepted statuses, confirmation counts, body limit or request timeouts resets that source's confirmed state without inventing RECOVERY. Poll/reminder interval changes retain the incident. Configuration changes require a rebuild in Docker; they are not a way to simulate recovery of an existing incident.

## Verify a notification

For a private/group destination, send `/status` from an allowed user in the configured chat/topic after startup. For a broadcast channel, test an outgoing incident instead of a command.

For a deliberate DOWN test, temporarily replace the enabled `example` endpoint's marker with an absent string in `config/checks.example.toml`, or add the field if it is absent. Keep only one active `marker` key inside that endpoint's `[[sites]]` table:

```toml
marker = "SENTINEL_MANUAL_TEST_MISSING_MARKER"
```

Rebuild and recreate the live service:

```bash
docker compose -p sentinel-live -f compose.yaml build sentinel
docker compose -p sentinel-live -f compose.yaml up -d --wait --force-recreate sentinel
```

With the example's 30-second interval and three-failure confirmation, expect DOWN roughly one minute after a fresh start; allow 1–2 minutes for checks and API requests. Read CLI status: state becomes DOWN, then `telegram.accepted` increases after provider acceptance. The message identifies the source and incident with UTC event/start times. Startup alone and the first successful check produce no alert.

Afterwards remove the temporary marker, rebuild and recreate again. This resets the source and does not produce RECOVERY for that test incident. To verify a matching RECOVERY, restore the same monitored endpoint while keeping its inventory/rules unchanged; two successful observations then resolve the same incident. No test command deletes state or creates a fake recovery.

A permanent failure (for example an invalid destination) is retained rather than retried forever. After fixing it, the terminal record does not resend on restart. A new unique temporary marker produces a new test incident; it does not repair or reroute the old record. Do not clear the database to repeat a test.

## Read status, logs and incident history

```bash
docker compose -p sentinel-live -f compose.yaml exec sentinel python -m sentinel_bot status
docker compose -p sentinel-live -f compose.yaml exec sentinel python -m sentinel_bot healthcheck
docker compose -p sentinel-live -f compose.yaml logs --tail=80 sentinel
```

`status` reads cached SQLite/health data without probing targets or contacting Telegram. `healthcheck` checks process ownership, current-run heartbeat and worker progress. A website DOWN or a handled provider outage can coexist with a healthy bot. Healthy does not mean a notification arrived; Docker does not restart a container solely because health becomes unhealthy.

The first success establishes UP without RECOVERY. A DOWN begins at the first failure of its confirmation series, and RECOVERY reuses that incident ID. Opposite observations, startup and stale gaps reset pending series. Restored readings remain stale until checked again; freshness expires after `2 × poll interval + total request deadline`. Unknown/missing readings are not silently healthy.

Telegram status reports `enabled`, `pending`, `retrying`, `failed`, `blocked`, `accepted`, `in_flight`, `paused` and a safe `last_error`. The counters describe bounded retained records. `last_error` may retain an older failed delivery even after a newer notification succeeds: `accepted: 1`, `failed: 1`, `pending: 0` is a valid outcome.

Routine probes and successful sends do not write INFO logs. Normalized warnings go to container stdout; durable incident/delivery evidence is in `/var/lib/sentinel/state.sqlite3` on `<project>_sentinel-state`. It retains the latest source observation, incident audit, notification IDs, attempt counts/times, provider-acceptance time and terminal errors. It does not retain every probe or a separate Telegram `message_id`. Website pending records are bounded to 80; incoming events have a separate 1,000-record budget with lifecycle reservations, for a combined maximum of 1,080. In-flight records remain bounded to 20, terminal delivery history and website incident audit to 200 each. Event receipts and terminal occurrence records have separate retention budgets.

DOWN precedes its RECOVERY. A failed opening blocks dependent recovery/reminders; a permanently failed reminder suppresses further reminders for that incident. Reminders require fresh active DOWN and an accepted opening/reminder. All sends share a FIFO with at least 3.1 seconds between starts; incidents have at most eight attempts. Backoff/jitter is capped at 300 seconds unless Telegram asks for more. Shared Retry-After pauses up to seven days survive restart; a larger hint stops explicitly. Current-process deadlines are monotonic; restart conservatively repeats saved delays/full reminder intervals and can add delay.

Provider acceptance is not proof a human read the message. A crash between external acceptance and local acknowledgement can duplicate a send. Accepted records are not deliberately resent. Interrupted attempts retain identity/destination; source changes/flapping coalesce only never-attempted obsolete records. Disabling Telegram pauses delivery. Changing chat/topic does not forward private old incidents: old pending records fail, and recovery stays bound to its opening route.

## Apply changes and stop a stack

| Change | Apply it |
| --- | --- |
| A baked main configuration, shared endpoint inventory or application code | `build sentinel`, then `up -d --wait --force-recreate sentinel` using the same project/files |
| `.env` credentials, destination, topic or selected config | `up -d --wait --force-recreate sentinel`; rebuild too if the TOML changed |
| Native TOML or exported environment | Stop the process and start it again with the same state path |

`docker compose restart` keeps the container's existing environment and image. There is no hot reload. `SENTINEL_CONTAINER_CONFIG` selects an already-baked path; the native `SENTINEL_CONFIG` variable has a separate scope.

Stop and remove only the live stack's container/network, retaining its database:

```bash
docker compose -p sentinel-live -f compose.yaml down
```

Use the matching project/files for the local or demo stack. `down --volumes` deletes the named state and should be reserved for an intentional reset. Each state directory has one owner; SIGTERM cancels supervised workers and waits for started state operations/network cleanup before releasing the stable-inode lock. Never delete the lock file to bypass ownership.

The default image runs UID/GID 10001, drops capabilities and uses a read-only root filesystem, bounded resources/logs and its own state volume. Poll-only operation has no listener; ingress is enabled explicitly and remains private to its network. The application uses no host mounts, Docker socket or privileged mode.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Telegram `enabled: false` | Selected config and `[telegram] enabled`; populated `.env` alone is insufficient |
| UNKNOWN, no pending alert | Wait for consecutive confirmation checks; inspect observation, freshness and local capacity deferral |
| HTTP 200 but error `marker` | Marker spelling/case, actual response body, maintenance/JavaScript page and body limit |
| Healthy but no message | Check configured source state and `accepted`/`pending`/`failed`; startup and initial UP do not send alerts |
| `bad_request` / `chat not found` | Full signed numeric Bot API destination, bot access to that chat/channel and an empty topic for broadcast channels; verify with `getChat` |
| `forbidden` | Bot membership and sending/posting permissions; confirm the token belongs to the bot you added |
| Fixed a permanent error but no retry | The prior record is terminal; wait for a new real incident or deliberately create a new test incident |
| `accepted` increased but `last_error` remains | A previous retained delivery failed; inspect counts/history rather than interpreting it as the new send's outcome |
| Token/webhook/conflicting poller error | Verify bot identity, run one poller per token and manage any existing webhook explicitly |
| API/network failures with local override | Use a dedicated outbound project and only `-f compose.yaml`; the Mailpit and synthetic-demo application networks are internal |
| `.env` edit has no effect | Recreate the container; check exported shell variables overriding `.env` without sharing expanded secrets |
| Status reports missing state | Start that same config/project/state first; status does not create a database |

## Native development and checks

Docker supplies the supported Linux runtime on other Docker-capable hosts. Runtime/build/development dependencies are pinned and hash-locked; the HTTP boundary uses aiohttp and cancellable aiodns.

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements-dev.lock
.venv/bin/python -m pip install --no-deps --no-build-isolation -e .
.venv/bin/python -m sentinel_bot check-config
.venv/bin/python -m sentinel_bot --config config/sites.example.toml check-config
```

Default native config is `config/idle.toml`, state `data/`. Start monitoring deliberately with `.venv/bin/python -m sentinel_bot --config config/sites.example.toml run`. Export Telegram variables first if selecting the Telegram example; native Python does not read Compose's `.env`. Use the same `--config` and `SENTINEL_STATE_DIR` in another terminal for status/health. See [CLI and environment precedence](configuration.md#configuration-loading).

| CLI command | Effect |
| --- | --- |
| `check-config` | Validate locally without requests or state initialization |
| `run` | Own state, supervise enabled monitors/Telegram workers and handle shutdown |
| `status` | Print cached JSON state and delivery counters |
| `healthcheck` | Check local runtime/worker progress, with no provider request |
| `test-email` | Send one fixed synthetic message only with local SMTP opt-in |

Run development checks without real provider credentials:

```bash
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/mypy src
.venv/bin/python -m pytest --cov=sentinel_bot --cov-report=term-missing
python3 scripts/docker_smoke.py
python3 scripts/monitoring_smoke.py
python3 scripts/telegram_smoke.py
python3 scripts/ingress_smoke.py
```

Tests use loopback HTTP/SMTP, fake Telegram and temporary SQLite. Foundation smoke checks separate local/server stacks, Mailpit, ownership/health/state/SIGTERM and container restrictions. Website smoke checks independent sources, outage/recovery and restart. Telegram smoke runs the real application/HTTP boundary against isolated local fixtures, including access checks, 429/retry and restart acceptance. Ingress smoke checks authenticated acceptance, duplicate/conflict behavior, terminal occurrences, ordered Telegram delivery/retry, push-only isolation and restart. Scripts clear inherited Telegram credentials, use unique temporary projects and remove only their own containers/volumes. They do not contact real Telegram or monitored public websites. Local passes do not establish a GitHub Actions run or deployment.
