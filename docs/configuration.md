# Configuration reference

Use TOML for non-secret runtime settings and the website inventory. Use environment variables for Telegram credentials and destinations. See the [running guide](running.md) for launch commands and local verification.

Website monitoring, Telegram delivery and cached commands are implemented. Incoming events, Linux metrics and production email have no supported configuration yet. The local SMTP command is a separate development check.

## Configuration loading

The main configuration must be valid UTF-8 TOML, at most **16 KiB**. Only `[runtime]`, optional `[telegram]` and optional inline `[[sites]]` tables are accepted. Set `runtime.sites_file` to load endpoint tables from a separate file instead; each file has its own 16 KiB limit. Unknown sections or keys fail validation. Durations accept finite integers or floats; counts require integers, not booleans. All ranges below are inclusive unless stated otherwise.

Native configuration selection, from highest to lowest priority:

1. `python -m sentinel_bot --config PATH COMMAND`.
2. Exported `SENTINEL_CONFIG`.
3. `config/idle.toml`, relative to the working directory.

Native execution does not load `.env`. Docker Compose reads `.env` for interpolation and explicitly passes supported variables into the container. In the normal project-root invocation, an exported shell value overrides the same value in `.env`; an explicit `--env-file` selects another interpolation file. See [Docker's interpolation rules](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/).

The supplied image contains these inventories:

| Repository file | Container path | Purpose |
| --- | --- | --- |
| [config/idle.toml](../config/idle.toml) | `/app/config.toml` and `/app/config/idle.toml` | Idle startup, no enabled websites |
| [config/sites.example.toml](../config/sites.example.toml) | `/app/config/sites.example.toml` | Public website monitoring |
| [config/telegram.example.toml](../config/telegram.example.toml) | `/app/config/telegram.example.toml` | Public website monitoring with Telegram enabled |
| [config/demo.toml](../config/demo.toml) | `/app/config/demo.toml` | Isolated synthetic websites |
| [config/checks.example.toml](../config/checks.example.toml) | `/app/config/checks.example.toml` | Shared endpoint tables; not a standalone main configuration |

Compose selects `/app/config.toml` by default. Set `SENTINEL_CONTAINER_CONFIG` to another **main configuration path in this table**, not the endpoint-only `checks.example.toml`. `compose.monitoring.yaml` explicitly selects `/app/config/demo.toml` instead. Host paths and arbitrary new TOML files are not mounted or copied automatically; a native `--config` can select another readable file, but the supplied Docker packaging includes only the listed examples. Rebuild after editing a packaged configuration or endpoint inventory.

## Complete website example

These two files show the runtime settings and website fields. The supplied website and Telegram configurations share [config/checks.example.toml](../config/checks.example.toml), so changing their endpoint inventory does not require duplicating it. The public URL and marker are illustrative; choose a repeatedly safe endpoint and stable text appropriate to your own service. To disable content matching, omit `marker` entirely.

Main configuration, such as `config/sites.example.toml`:

```toml
[runtime]
mode = "monitor"
state_dir = "data"
heartbeat_interval_seconds = 1.0
health_timeout_seconds = 5.0
shutdown_timeout_seconds = 5.0
max_concurrency = 5
sites_file = "checks.example.toml"

[telegram]
enabled = false
```

Endpoint inventory, `config/checks.example.toml`, relative to the main configuration's directory:

```toml
[[sites]]
alias = "public_site"
url = "https://example.com/"
method = "GET"
marker = "Example Domain"
status_min = 200
status_max = 399
poll_interval_seconds = 30
connect_timeout_seconds = 5
total_timeout_seconds = 10
failure_confirmation = 3
recovery_confirmation = 2
reminder_interval_seconds = 1800
max_body_bytes = 65536
enabled = true
```

The endpoint file accepts only repeated `[[sites]]` tables. Do not place `[runtime]`, `[telegram]` or another file include in it. Inline `[[sites]]` in a main configuration remains supported: omit `runtime.sites_file` and put the endpoint tables after the main settings. Combining an external inventory with any inline `sites` field is invalid, even when the inline list is empty. There is no merge or recursive inclusion.

### `[runtime]`

| Key | Default | Accepted value and effect |
| --- | --- | --- |
| `mode` | Required | Exactly `"idle"`, `"monitor"` or `"demo"` |
| `state_dir` | `"data"` | Nonempty path, at most 2,048 characters, no NUL. An existing path must be a directory. Startup must be able to write it. Exported `SENTINEL_STATE_DIR` overrides this value. |
| `heartbeat_interval_seconds` | `1.0` | `0.1`–`60`; interval between local heartbeat updates |
| `health_timeout_seconds` | `5.0` | `0.2`–`300`; must be at least twice `heartbeat_interval_seconds` |
| `shutdown_timeout_seconds` | `5.0` | `0.1`–`30`; application worker shutdown deadline |
| `max_concurrency` | `5` | Integer `1`–`5`; shared simultaneous website request limit |
| `sites_file` | Omitted | Optional nonempty relative inventory path, at most 2,048 characters, resolved from the main configuration's directory. Absolute paths, `..` segments, backslashes, NUL/control characters and paths/symlinks escaping that directory are rejected. Cannot be combined with inline `[[sites]]`. |

`idle` permits no enabled sites and can optionally run Telegram alone. `monitor` requires at least one enabled site. `demo` also requires an enabled site, permits only the named synthetic website origin and rejects enabled live Telegram. Disabled site rows remain subject to validation in every mode.

The default Compose shutdown grace period is 15 seconds. If you raise the application shutdown deadline, also allow enough container grace time for cleanup. A larger health timeout changes heartbeat freshness; it does not alter the fixed Telegram worker progress threshold or website progress formula.

## Website checks: sites

At most **20 rows**, including disabled rows, are accepted from the selected inline or external inventory. Aliases must be unique across all rows. Each table is one independently scheduled endpoint; multiple URLs on the same service can have separate aliases, intervals, confirmation rules and incidents.

| Key | Default | Accepted value and effect |
| --- | --- | --- |
| `alias` | Required | 1–32 ASCII characters matching `[A-Za-z][A-Za-z0-9_-]{0,31}` |
| `url` | Required | Bounded HTTP(S) URL under the address policy below; at most 2,048 characters |
| `method` | `"GET"` | Exactly `"GET"` or `"POST"`; other methods and lowercase names are rejected |
| `json_body` | Omitted | Optional JSON-compatible TOML table for POST only. Encoded as canonical UTF-8 JSON, at most 4,096 bytes; top-level arrays/scalars and non-JSON values are rejected. Omit for a bodyless POST; GET forbids this field, including an empty table. |
| `marker` | Omitted | Nonempty, case-sensitive UTF-8 substring required in the response body; at most `min(max_body_bytes, 4096)` UTF-8 bytes |
| `status_min` | `200` | Integer `100`–`599`; lowest accepted final HTTP status |
| `status_max` | `399` | Integer `status_min`–`599`; highest accepted final HTTP status |
| `poll_interval_seconds` | `30` | `0.1`–`86400`; sequential per-site polling interval |
| `connect_timeout_seconds` | `5` | `0.1`–`30`; must not exceed `total_timeout_seconds` |
| `total_timeout_seconds` | `10` | `0.2`–`120`; network deadline across DNS, redirects and body reads |
| `failure_confirmation` | `3` | Integer `1`–`100`; consecutive failures needed to confirm DOWN |
| `recovery_confirmation` | `2` | Integer `1`–`100`; consecutive successes needed to confirm recovery |
| `reminder_interval_seconds` | `1800` | `1`–`604800`; interval after an accepted opening/reminder for a fresh active incident |
| `max_body_bytes` | `65536` | Integer `1`–`65536`; separate upper bound for encoded and decoded marker bodies |
| `enabled` | `true` | Boolean; `false` excludes the validated row from polling |

### URL and marker behavior

Live URLs use ports **80 or 443**, public addresses and verified TLS for HTTPS. Credentials, query strings, fragments, backslashes, ASCII whitespace/control characters, localhost and private/reserved addresses are rejected. Every connection validates all DNS answers. GET follows at most three redirects, only within the same origin: scheme, host and effective port. POST never follows/replays redirects; HTTP 301, 302, 303, 307 and 308 fail with `redirect`, including when the configured successful status range includes that code. There is no insecure-TLS, private-host, proxy, authentication-header or arbitrary-port option for website checks. Demo mode permits only `http://site-fixture:8080`, reached through the synthetic Compose network.

A marker is a literal substring of the UTF-8 response body, not a regular expression, CSS selector or JavaScript check. It must fit within the configured body limit. Empty `marker = ""` is invalid. Omit the key when HTTP status alone is sufficient; in that case the checker closes the response without reading the body. Marker checks support identity, gzip and deflate encoding, with encoded and decoded sizes each bounded. Invalid UTF-8, unsupported/invalid encoding or missing text fails the marker check; an oversized body reports `body_limit`.

The first successful reading sets UP without a recovery notification. Consecutive confirmations determine later DOWN/RECOVERY transitions. Missing/restored readings remain stale until a new check completes. Freshness lasts `2 × poll_interval_seconds + total_timeout_seconds`. Website worker health allows `2 × poll_interval_seconds + 2 × total_timeout_seconds`, including a separately bounded shared-slot wait. Exhausted local request capacity defers a check and resets pending confirmations; it does not invent a website failure.

### POST health probes

Add a separate endpoint table for a public health probe that is safe to repeat:

```toml
[[sites]]
alias = "api_probe"
url = "https://example.com/health/probe"
method = "POST"
json_body = { probe = "availability" }
poll_interval_seconds = 60
status_min = 200
status_max = 299
enabled = false
```

Replace the illustrative URL and explicitly enable it only when the endpoint is designed for repeated probes. The supplied POST example is disabled and sends nothing. It must not create orders, payments, accounts or other business actions. Requests run on the endpoint's polling interval, without an immediate transport retry; a timeout leaves the remote outcome unknown and does not guarantee the server did no work. This is periodic availability checking, not a transactional workflow or API client with login/headers/secrets.

`json_body` may contain JSON objects, arrays, strings, finite numbers and booleans supported by TOML; TOML date/time values and non-finite floats are invalid. The top level must be an object. Serialization sorts object keys, removes optional whitespace and preserves UTF-8 text; the 4,096-byte limit includes the complete serialized body. Content-Type is `application/json` only when a JSON body is present. Omitting the field sends a POST without a JSON body. JSON responses can be checked by literal `marker` text only; there is no JSON-path assertion.

## Telegram

The only TOML key in `[telegram]` is `enabled`, a boolean defaulting to `false`. Omit the table to leave Telegram disabled. Credentials are validated and workers start only when enabled. For Telegram without website polling, a complete inventory is:

```toml
[runtime]
mode = "idle"
state_dir = "data"

[telegram]
enabled = true
```

This inventory still requires the environment variables below. There is one fixed destination, not separate command and alert chats.

| Variable | Requirement |
| --- | --- |
| `SENTINEL_TELEGRAM_BOT_TOKEN` | Required when enabled. Use the exact BotFather token, without a URL prefix or surrounding whitespace. Validation accepts 1–20 non-leading-zero prefix digits, a colon and 20–128 letters, digits, `_` or `-`. |
| `SENTINEL_TELEGRAM_CHAT_ID` | Required nonzero signed decimal integer with absolute value below `2^52`. Use the complete Bot API chat ID, including its sign. No `+`, leading zeros, whitespace, invite link or `@username`. |
| `SENTINEL_TELEGRAM_ALLOWED_USER_IDS` | Required comma-separated list of 1–20 unique positive user IDs, each below `2^52`. Whitespace around list entries is accepted; the entire value is bounded to 400 characters. IDs have no `+` or leading zeros. |
| `SENTINEL_TELEGRAM_TOPIC_ID` | Optional; omitted/empty means no topic restriction. Sentinel supports a positive integer up to `2^31 − 1` for a group destination with a negative chat ID. Use the topic's message-thread ID. Leave empty for a private chat or broadcast channel. |

Use the exact identifier returned for the intended chat by trusted Telegram tooling; a copied invite code or an ID from another Telegram interface is not a substitute. The [Bot API Chat object](https://core.telegram.org/bots/api#chat) documents its signed numeric identifier.

| Destination | Alerts | Commands |
| --- | --- | --- |
| Private chat | The user must first start the bot | Only allowed human users in that chat |
| Group/supergroup | Add the bot and permit sending messages | Allowed human users; exact topic when configured |
| Broadcast channel, including a private channel | Add the bot as an administrator with permission to post | Channel posts and anonymous senders are ignored; use CLI status |

Channel posting permission is Telegram's [`can_post_messages` administrator right](https://core.telegram.org/bots/api#chatadministratorrights). A numeric user allowlist is still required by Sentinel when the selected destination is a channel. The current single-destination setting does not provide a second private chat for channel commands.

Without a configured topic, group commands are accepted throughout the configured chat and replies stay in the incoming topic; incident alerts use the chat without a topic. With a topic configured, commands and alerts use that exact topic. Commands from other chats/users/topics, bots, anonymous senders, edited messages and channel posts are ignored. `/help`, `/status`, `/sites`, `/servers` and `/server <alias>` read cached data; server commands currently report no configured server sources.

Only one poller may use a bot token. Existing webhooks are refused, never deleted automatically. `check-config` validates local values without contacting Telegram. Starting the application validates the bot and delivers eligible queued incidents; it does not send an automatic greeting or test message. The production API origin is fixed to verified `https://api.telegram.org` and cannot be replaced through configuration.

## Environment scopes

| Variable | Native execution | Supplied Compose setup |
| --- | --- | --- |
| `SENTINEL_CONFIG` | Exported default inventory path; `--config` takes precedence | Container value comes from `SENTINEL_CONTAINER_CONFIG`; a host `.env` value for `SENTINEL_CONFIG` does not select the container inventory |
| `SENTINEL_STATE_DIR` | Overrides TOML `runtime.state_dir` | Fixed to `/var/lib/sentinel` on the named state volume; the host `.env` value does not override it |
| `SENTINEL_CONTAINER_CONFIG` | Not used by the Python application | Selects a baked inventory; default `/app/config.toml`; the monitoring override explicitly selects demo configuration |
| `SENTINEL_TELEGRAM_*` above | Must be exported into the process environment | Explicitly interpolated from the shell or `.env`; absent values become empty |
| `MAILPIT_HTTP_PORT` | Not used by the Python application | Mailpit loopback UI host port, default `8025`; normally `1`–`65535`, with `0` requesting automatic Docker allocation; relevant only with the local override |
| `COMPOSE_PROJECT_NAME` | Not used by the Python application | Compose project/container/network/volume namespace; `docker compose -p NAME` selects it explicitly |

The base `compose.yaml` permits outbound HTTPS for real websites and Telegram. The automatic `compose.override.yaml` adds Mailpit and makes the application network internal. The explicit monitoring override also uses an internal network. Use the launch commands for the intended network in the [running guide](running.md); changing a configuration filename does not change network isolation. Distinct project names use separate state volumes, but must not run competing pollers with the same Telegram token.

### Local SMTP variables

These values affect only the deliberately invoked `test-email` command. They are not incident email settings and provide no production provider integration.

| Variable | Default | Accepted value |
| --- | --- | --- |
| `SENTINEL_SMTP_TEST_ENABLED` | Disabled | Exactly `true` enables the local test |
| `SENTINEL_SMTP_HOST` | `mailpit` | Exactly `mailpit`, `localhost`, `127.0.0.1` or `::1` |
| `SENTINEL_SMTP_PORT` | `1025` | Integer `1`–`65535` |
| `SENTINEL_SMTP_TIMEOUT_SECONDS` | `5` | Finite number `0.1`–`10`; bounded overall test deadline |

The local Compose override explicitly sets `true`, `mailpit`, `1025` and `5` in the container. Adding different SMTP values to `.env` alone does not replace those literal Compose settings. Native use requires exported variables and a reachable local sink. Sender and recipient are fixed synthetic `example.test` addresses; there are no configurable recipient, authentication, STARTTLS or production SMTP options. Startup, monitoring, status and health never send test mail.

Mailpit's separate service settings live in `compose.override.yaml`. It publishes only its UI on loopback, uses bounded temporary storage, retains at most 100 messages for 24 hours, accepts messages up to 1 MiB for `example.test` recipients only and has no configured relay. Recreating Mailpit clears its inbox. These are sink settings, separate from Sentinel's inventory and Telegram delivery.

## Fixed limits and derived behavior

The following application limits are not configurable TOML or environment fields:

| Area | Current behavior |
| --- | --- |
| Inventory/file bounds | 20 site rows, 16 KiB per main/inventory file, 32-character aliases, 4,096-byte POST JSON body; upper limits cannot be raised through config |
| Website redirects | GET: at most 3, same origin only. POST: no redirect follow/replay. |
| Durable delivery queue | At most 80 pending and 20 in-flight records; capacity failure rolls back the new observation/transition |
| History | At most 200 terminal delivery records and 200 incident-audit records; not a complete outage archive |
| Telegram send pacing | One FIFO sender; at least 3.1 seconds between incident/reply send starts |
| Incident attempts | At most 8; backoff with jitter capped at 300 seconds unless Telegram requires longer |
| Provider cooldown | Shared by polling-triggered and send-triggered 429s; persisted Retry-After up to 7 days |
| Command rates | 6/minute per user, burst 2; 30/minute globally, burst 5 |
| Command input | At most 256 characters for command parsing; arguments cannot select URLs, recipients or new sources |
| Command reply queue | 20 in-memory replies, at most 3 attempts each; overflow/restart can lose a reply |
| Rendered messages | At most 3,500 UTF-16 units, plain text with control characters removed and previews disabled |
| Telegram transport | At most 20 updates per poll, 20-second long poll, 1 MiB response bound, 10-second ordinary request deadline and 25-second polling request deadline |
| Worker progress | Telegram workers must progress within 45 seconds; expected network outages/cooldowns continue updating progress |
| Polling checkpoint | Durable update offset, expired after 24 hours without a handled update |

Current-process scheduling, retries, freshness and reminder deadlines use monotonic time. Stored event and acceptance times use UTC. Restart conservatively repeats saved retry/cooldown durations and an initial send interval. Confirmed incidents persist, pending confirmation series reset and restored observations remain stale until checked again.

DOWN must be accepted before its matching RECOVERY. A permanently failed opening blocks its dependent recovery; restarting does not retry that terminal failure. A terminal reminder failure suppresses more reminders for that incident. A later successful send does not erase an older failure from retained history, so status can show accepted records and an older `last_error` together. Provider acceptance is not exactly-once delivery: a crash before local acknowledgement can cause a duplicate.

## Applying changes

Configuration is loaded at process startup; there is no hot reload.

| Change | How to apply | State effect |
| --- | --- | --- |
| Baked TOML configuration or endpoint inventory | Edit the supported repository file, then run `up -d --build --wait` with the same Compose file/project selection | Rebuilds the image and reads its configuration/inventory on startup |
| `.env` token, chat, topic or container inventory selection | Run Compose `up -d --force-recreate --wait`; no image rebuild is needed if baked files are unchanged | Container receives the new environment; `restart` alone preserves its old environment |
| Native TOML or exported environment | Stop and start the native process with the intended values | New startup reads the values |
| Ordinary container restart | Reuses the existing image/environment and the same named state volume | Preserves incidents and delivery history; resets pending confirmations and freshness |
| Site URL, HTTP method, JSON body, marker, accepted status range, either confirmation count, body limit or either request timeout | Apply the inventory change as above | Resets that site's confirmed state to UNKNOWN, cancels obsolete never-attempted records and does not invent RECOVERY; attempted/in-flight outcomes remain protected |
| Poll/reminder interval | Apply the inventory change | Preserves confirmed incident identity; restart begins fresh scheduling/reminder deadlines |
| Site disabled/removed or alias renamed | Apply the inventory change | Removes the old active source state and obsolete never-attempted records; a new alias starts as a new source |
| Telegram first enabled | Apply TOML and environment | Binds never-attempted logical-operator records to the configured destination without pretending they were already sent |
| Telegram disabled | Apply TOML | Stops its workers and pauses bound records |
| Telegram chat/topic changed | Apply environment | Old pending records fail with `destination_changed`; old incidents/history are not forwarded to the new destination |

Removing a marker is a check-semantics change, so it resets the source rather than proving recovery of its previous incident. Successful sends and terminal failures are not reset merely by rebuilding or restarting. Keep the same project name and volume to retain state; `down` preserves the named volume, while `down --volumes` intentionally deletes it.

Moving identical endpoint tables from inline configuration to `sites_file` preserves their existing GET incident identity. Changing the method or JSON body changes check semantics and resets only that endpoint. Reordering keys in the same JSON object does not change its canonical body or fingerprint.

Validate locally before running:

```bash
docker compose -f compose.yaml run --rm --no-deps sentinel check-config
```

This checks the selected inventory and enabled-channel credentials without sending requests or creating application state. Rebuild first when validating edited baked TOML. For status, health, logs and operational examples, continue with the [running guide](running.md).
