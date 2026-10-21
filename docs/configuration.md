# Configuration reference

Use TOML for non-secret runtime settings and website/server inventories. Use environment variables for channel/exporter credentials, email addresses and Telegram destinations. See the [running guide](running.md) for launch commands and local verification.

Website monitoring, Linux Node Exporter metrics, incoming events, Telegram delivery, independent incident email and cached commands are implemented. The local SMTP command remains a separate development check. The [API guide](api.md) describes incoming request and response semantics.

## Configuration loading

The main configuration must be valid UTF-8 TOML, at most **16 KiB**. Supported sections are `[runtime]`, optional `[telegram]`, `[email]`, `[ingress]`, `[[sources]]`, `[[servers]]` and inline `[[sites]]`. Set `runtime.sites_file` to load website endpoint tables from a separate file instead; each file has its own 16 KiB limit. Server tables belong in the main file. Unknown sections or keys fail validation. Durations accept finite integers or floats; counts require integers, not booleans. All ranges below are inclusive unless stated otherwise.

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
| [config/ingress.example.toml](../config/ingress.example.toml) | `/app/config/ingress.example.toml` | Push-only source and configured delivery |
| [config/email.example.toml](../config/email.example.toml) | `/app/config/email.example.toml` | Public websites with production email |
| [config/email.local.toml](../config/email.local.toml) | `/app/config/email.local.toml` | Synthetic websites with Mailpit incident email |
| [config/servers.example.toml](../config/servers.example.toml) | `/app/config/servers.example.toml` | Linux Node Exporter through an authenticated HTTPS endpoint |
| [config/servers.local.toml](../config/servers.local.toml) | `/app/config/servers.local.toml` | Synthetic Linux readings and Mailpit incidents |
| [config/checks.example.toml](../config/checks.example.toml) | `/app/config/checks.example.toml` | Shared website/Telegram/email endpoint tables; not a standalone main configuration |

Compose selects `/app/config.toml` by default. Set `SENTINEL_CONTAINER_CONFIG` to another **main configuration path in this table**, not the endpoint-only `checks.example.toml`. `compose.monitoring.yaml` explicitly selects `/app/config/demo.toml` instead. Host paths and arbitrary new TOML files are not mounted or copied automatically; a native `--config` can select another readable file, but the supplied Docker packaging includes only the listed examples. Rebuild after editing a packaged configuration or endpoint inventory.

## Complete website example

These two files show the runtime settings and website fields. The supplied website, Telegram and production email configurations share [config/checks.example.toml](../config/checks.example.toml), so changing their endpoint inventory does not require duplicating it. The public URL and marker are illustrative; choose a repeatedly safe endpoint and stable text appropriate to your own service. To disable content matching, omit `marker` entirely.

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
| `max_concurrency` | `5` | Integer `1`–`5`; shared simultaneous website/exporter request limit |
| `sites_file` | Omitted | Optional nonempty relative inventory path, at most 2,048 characters, resolved from the main configuration's directory. Absolute paths, `..` segments, backslashes, NUL/control characters and paths/symlinks escaping that directory are rejected. Cannot be combined with inline `[[sites]]`. |

`idle` permits no enabled monitoring inputs and can optionally run Telegram or email alone. `monitor` requires an enabled website, server or incoming source. `demo` requires an enabled synthetic website or server, permits only named fixture origins and rejects enabled live Telegram, Mailchimp email or ingress; local SMTP incident email is allowed. Disabled inventory rows remain subject to structural validation in every mode; disabled servers do not read token values.

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

## Linux servers: servers

Configure each explicitly polled Node Exporter endpoint with a `[[servers]]` table in the main file. See [servers.example.toml](../config/servers.example.toml) and the [operating guide](running.md#monitor-linux-servers). No Prometheus server, shell/SSH access or host mount is required by Sentinel. A push-only incoming source cannot share an outbound polling alias; a hybrid source may explicitly combine an enabled website or server with incoming events.

Physical website, server and incoming-source aliases share a **20-source** bound, including disabled rows. Website and server aliases must be distinct. Enabled websites plus expanded server incident rules must total at most **40**. A server always has an exporter-availability rule; enabled CPU, memory and filesystem alerts add one rule each. Setting alert flags to false leaves their readings available as informational data.

| `[[servers]]` key | Default | Accepted value and effect |
| --- | --- | --- |
| `alias` | Required | Same 1–32 ASCII identifier format as websites; cannot select a target through Telegram. |
| `url` | Required | Fixed HTTPS URL, at most 2,048 characters, with any valid port `1`–`65535` and an operator-selected path such as `/metrics`; no credentials, query, fragment, whitespace, controls or backslash. |
| `token_env` | Required for real endpoints | Uppercase environment variable name matching `[A-Z][A-Z0-9_]{0,127}`. Enabled real sources require a URL-safe 32–128-character value; no token in TOML. Demo fixtures need no token. |
| `allowed_cidrs` | `[]` | Up to 16 unique canonical IPv4/IPv6 networks for this source's private addresses; public addresses are allowed without a list. Loopback, link-local, multicast, unspecified and reserved targets cannot be enabled through a CIDR. |
| `poll_interval_seconds` | `30` | Finite `0.1`–`86400`; independent sequential polling, without overlapping or accumulated missed polls. |
| `connect_timeout_seconds` | `5` | `0.1`–`30`, no greater than the total deadline. |
| `total_timeout_seconds` | `10` | `0.2`–`120`; includes DNS, transport, response reading and parsing. |
| `failure_confirmation` | `3` | Integer `1`–`100`; failures/high readings to confirm an incident. |
| `recovery_confirmation` | `2` | Integer `1`–`100`; successful/low readings to confirm recovery. |
| `reminder_interval_seconds` | `1800` | `1`–`604800`; requires fresh valid readings and an accepted opening/reminder for that destination. |
| `cpu_alerts` / `memory_alerts` | `true` | Boolean; `false` collects the reading without creating its resource incident rule. |
| `cpu_trigger_percent` / `memory_trigger_percent` | `85` | Finite `0`–`100`; at or above this value counts toward DOWN. |
| `cpu_recovery_percent` / `memory_recovery_percent` | `75` | Finite `0`–`100`, strictly below the corresponding trigger; at or below this value counts toward recovery. |
| `enabled` | `true` | Boolean; `false` excludes the structurally validated source from polling and reads no token value. |

Each connection validates all DNS answers against that server's address policy. TLS certificate/hostname verification is required. Credentials are sent only to its configured origin; redirects, ambient proxies, cookies, arbitrary headers and compression are not supported. Private HTTP is limited to `metrics-fixture:9100` or `metrics-fixture:9101` in demo mode. Real endpoints require an HTTPS Bearer-auth proxy; Node Exporter's native basic-auth setting alone does not satisfy this contract.

Select at most four exact filesystems per server, with unique mountpoints and aliases:

```toml
[[servers.filesystems]]
alias = "root"
mount = "/"
trigger_percent = 85
recovery_percent = 75
alerts = true
```

`alias` uses the same bounded identifier format. `mount` is an absolute path, at most 256 characters, without control characters. Threshold ranges and hysteresis match CPU/memory; `alerts = false` keeps this selected filesystem informational. Omitted filesystems are not discovered or alerted automatically. Duplicate/ambiguous series, collector failure, device errors and read-only filesystems yield unknown usage instead of a healthy zero.

| Reading | Meaning |
| --- | --- |
| CPU busy | `100 × (1 − idle_delta / total_delta)` over two valid scrapes in the current run; excludes `guest`/`guest_nice`, includes iowait as busy. |
| Memory used | `100 × (1 − MemAvailable / MemTotal)`; not `MemFree`. |
| Filesystem capacity unavailable | `100 × (1 − available_bytes / size_bytes)`; includes space reserved from non-root users, not exact occupied bytes. |
| Uptime | UTC scrape time minus valid boot time; informational, no uptime alert rule. |

CPU requires a matching CPU/mode set, a stable boot time and nondecreasing counters with a positive total delta. Startup, counter reset, changed CPU set, invalid scrape or gap beyond freshness discards the baseline and warms up again. Missing/invalid/NaN/infinite values and failed required collectors remain unknown; one unknown reading does not hide another valid reading from the scrape. Responses are bounded to 1 MiB, 10,000 samples, 512 families, 16 KiB per line, eight labels per sample and 256 CPUs. Only normalized selected readings are persisted; raw exposition, devices, URLs and credentials are excluded from status/notifications.

Exporter availability and each enabled resource rule have separate confirmed state and incident IDs. A valid response can leave individual readings unknown. Only new distinct readings advance confirmations. Values between trigger and recovery retain confirmed state and clear a pending transition count; unknown data also clears that count and cannot open or resolve an incident. Existing resource incidents remain visible with unknown/stale current data and produce no reminders until fresh valid readings resume. Initial healthy observations create no RECOVERY. Freshness is `2 × poll interval + total deadline`; local worker health is independent of the monitored server's availability.

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

Without a configured topic, group commands are accepted throughout the configured chat and replies stay in the incoming topic; incident alerts use the chat without a topic. With a topic configured, commands and alerts use that exact topic. Commands from other chats/users/topics, bots, anonymous senders, edited messages and channel posts are ignored. `/help`, `/status`, `/sites`, `/servers` and `/server <alias>` read cached data. Server commands expose configured aliases, exporter availability, freshness, resource readings and confirmed incidents without making requests.

Only one poller may use a bot token. Existing webhooks are refused, never deleted automatically. `check-config` validates local values without contacting Telegram. Starting the application validates the bot and delivers eligible queued incidents; it does not send an automatic greeting or test message. The production API origin is fixed to verified `https://api.telegram.org` and cannot be replaced through configuration.

## Email

Enable email independently of Telegram. The production example is [config/email.example.toml](../config/email.example.toml); [config/email.local.toml](../config/email.local.toml) uses the separate Mailpit sink for offline incident delivery. In either case, startup, health and status send no test/greeting email.

```toml
[email]
enabled = true
provider = "mailchimp"
request_timeout_seconds = 10
```

| `[email]` key | Default | Supported value and meaning |
| --- | --- | --- |
| `enabled` | `false` | Boolean; disabled email reads no credentials/addresses and starts no sender. |
| `provider` | `"mailchimp"` | `"mailchimp"` for Mailchimp Transactional HTTP API, or `"local_smtp"` for a local synthetic sink. |
| `request_timeout_seconds` | `10` | Finite `0.1`–`10`; overall deadline for one recipient attempt. |
| `local_host` | `"mailpit"` | Local provider only: exactly `mailpit`, `localhost`, `127.0.0.1` or `::1`. |
| `local_port` | `1025` | Local provider only: exactly `1025`; no arbitrary SMTP relay. |

Local-only keys are rejected for `mailchimp`, even when they equal a default. Demo mode permits only local SMTP; production Mailchimp cannot start in an isolated synthetic demo.

| Environment variable | Requirement |
| --- | --- |
| `SENTINEL_EMAIL_API_KEY` | Required only for enabled `mailchimp`; 16–128 ASCII letters, digits, `_` or `-`. Keep the private Transactional API key outside TOML and Git. |
| `SENTINEL_EMAIL_FROM` | Required when email is enabled; one plain ASCII mailbox up to 254 characters, with a local part up to 64; no display name, surrounding whitespace or control characters. Use an authenticated sending domain in production. |
| `SENTINEL_EMAIL_RECIPIENTS` | Required when enabled; 1–5 unique plain ASCII mailboxes, separated by commas. The complete value is bounded to 1,279 characters. Spaces around list entries are accepted; tabs, other control characters, display names and header injection are rejected. |

The local SMTP sender and every recipient must use the synthetic `example.test` domain. The optional [compose.email.yaml](../compose.email.yaml) supplies these literal addresses and clears inherited production credentials for the [offline incident email demo](running.md#offline-incident-email-demo). A real mailbox or external SMTP host cannot be selected through this transport. The deliberately invoked `test-email` command has its own opt-in and fixed address pair.

Mailchimp requests use the fixed verified HTTPS origin `https://mandrillapp.com`, the Transactional `/api/1.4/messages/send` API and one recipient per attempt. There is no configurable API URL, proxy, marketing campaign API or provider SDK. Email is plain text with bounded subject/body and no source-controlled headers or attachments. Incoming payloads cannot choose sender, recipients or transport options.

`sent`, `queued` and `scheduled` are provider acceptance. Invalid recipients and recipient-specific rejections are permanent failures; sender-domain configuration failures suspend the channel instead. Acceptance does not prove inbox delivery or a human read. Authentication or sending-domain configuration failures suspend email for the current run; pending records remain durable and the other channel continues. Correct the environment/configuration and recreate the container to resume within the same eight-attempt budget. Quota/rate responses pause email with bounded retry; exhausted attempts become terminal failures. Each recipient has independent persisted attempts/status: accepted recipients are not resent because another recipient or Telegram retries. Transport failure can leave the external result unknown, so a crash between provider acceptance and local acknowledgement can still duplicate email. See the [provider's send contract](https://mailchimp.com/developer/transactional/api/messages/send/).

Full Mailchimp Transactional use requires an eligible Standard/Premium Marketing plan, Transactional credits and an authenticated sending domain; a limited trial has its own restrictions. Sentinel's local demo requires no paid account. Confirm the [current product requirements and pricing](https://mailchimp.com/pricing/transactional-email/) and [domain authentication](https://mailchimp.com/developer/transactional/docs/authentication-delivery/) before enabling real delivery.

## Incoming events

See [`config/ingress.example.toml`](../config/ingress.example.toml) for a complete push-only configuration and the [API guide](api.md) for payloads. Incoming sources are configured separately from website probes. An enabled receiver requires at least one enabled notification channel (Telegram or email) and at least one source. Email-only sources require no Telegram credentials or poller.

| `[ingress]` key | Default | Supported range or meaning |
| --- | --- | --- |
| `enabled` | `false` | Boolean; disabled receiver starts no listener and reads no source token values. |
| `host` | `"127.0.0.1"` | `127.0.0.1`, `0.0.0.0`, `::1` or `::`; remote access belongs behind an HTTPS proxy. |
| `port` | `8080` | Integer `1024`–`65535`; no host port is published by the ingress override. |
| `max_body_bytes` | `16384` | Integer `256`–`16384`; complete uncompressed JSON body. |
| `max_summary_chars` | `1000` | Integer `1`–`1000`; nonblank single-line summary without control characters. |
| `body_timeout_seconds` | `5` | `0.1`–`30`; body-read deadline. |
| `max_concurrency` | `10` | Integer `1`–`10`; excess requests receive `503`. |
| `source_rate_per_minute` | `60` | Integer `1`–`600`; authenticated per-source request budget. |
| `source_burst` | `10` | Integer `1`–`100`; source token-bucket capacity. |
| `global_rate_per_minute` | `300` | Integer `1`–`3000`; shared authenticated request budget. |
| `global_burst` | `50` | Integer `1`–`500`; shared token-bucket capacity. |
| `max_event_age_seconds` | `86400` | `1`–`604800`; new-event age limit. |
| `future_skew_seconds` | `300` | `0`–`3600`; permitted sender clock lead. |
| `retention_seconds` | `172800` | `172800`–`1209600`; at least age plus future skew, for receipts and terminal occurrences. |
| `max_receipts` / `max_receipts_per_source` | `10000` / `1000` | Positive integers up to these defaults; retained duplicate identities. |
| `max_tombstones` / `max_tombstones_per_source` | `10000` / `1000` | Positive integers up to these defaults; retained terminal occurrence identities. |
| `max_pending` / `max_pending_per_source` | `1000` / `200` | Positive integers up to these defaults; incoming delivery budget, separate from 80 polled website/server slots. |
| `max_active_incidents` / `max_active_incidents_per_source` | `200` / `50` | Positive integers up to these defaults; active pushed occurrences. |

Per-source rates, bursts and capacities cannot exceed their global equivalents. Pending budgets must cover opening plus resolution for every configured active occurrence; receipt/tombstone budgets must also cover lifecycle reservations. Lowering limits does not erase accepted state. An already accepted active occurrence retains its matching resolution capacity; new admissions fail when they would spend protected slots. Protected receipts/tombstones are not pruned before retention to admit more events. Rate checks still apply to duplicate requests.

| `[[sources]]` key | Required/default | Meaning |
| --- | --- | --- |
| `alias` | Required | Same 1–32-character identifier rules as website aliases. |
| `mode` | `"push"` | `"push"` has no target; `"hybrid"` requires an explicit enabled website or server with the same alias. |
| `allowed_types` | Required | 1–20 unique lowercase identifiers matching `[a-z][a-z0-9_.-]{0,63}`. |
| `token_env` | Required | Distinct uppercase environment name, at most 128 characters; value stays outside TOML. |
| `enabled` | `true` | Boolean; disabled sources cannot authenticate. |

At most 20 distinct configured source aliases are allowed across sites, servers and incoming sources, including disabled entries. A push-only alias cannot collide with a website or server; a hybrid alias explicitly combines an enabled polled input with events. Source entries cannot define URLs, ports, credentials for outbound access or recipients. Tokens are distinct URL-safe ASCII strings of 32–128 characters, read only for enabled receiver sources, and excluded from repr, logs, status and persistence. Rotate them through environment/recreation. `compose.ingress.yaml` forwards only the example's `SENTINEL_SOURCE_APP_TOKEN`; explicitly add forwarding for each additional source credential.

## Environment scopes

| Variable | Native execution | Supplied Compose setup |
| --- | --- | --- |
| `SENTINEL_CONFIG` | Exported default inventory path; `--config` takes precedence | Container value comes from `SENTINEL_CONTAINER_CONFIG`; a host `.env` value for `SENTINEL_CONFIG` does not select the container inventory |
| `SENTINEL_STATE_DIR` | Overrides TOML `runtime.state_dir` | Fixed to `/var/lib/sentinel` on the named state volume; the host `.env` value does not override it |
| `SENTINEL_CONTAINER_CONFIG` | Not used by the Python application | Selects a baked inventory; default `/app/config.toml`; the monitoring override explicitly selects demo configuration |
| `SENTINEL_TELEGRAM_*` above | Must be exported into the process environment | Explicitly interpolated from the shell or `.env`; absent values become empty |
| `SENTINEL_EMAIL_*` above | Must be exported into the process environment | Explicitly interpolated from the shell or `.env`; offline email/metrics overrides supply fixed synthetic addresses |
| `SENTINEL_SERVER_NODE_TOKEN` | Value of the example server's `token_env`; exported only for enabled real polling | Forwarded explicitly by the base file; extra token names require extra Compose forwarding |
| `MAILPIT_HTTP_PORT` | Not used by the Python application | Mailpit loopback UI host port, default `8025`; normally `1`–`65535`, with `0` requesting automatic Docker allocation; relevant only with the local override |
| `COMPOSE_PROJECT_NAME` | Not used by the Python application | Compose project/container/network/volume namespace; `docker compose -p NAME` selects it explicitly |

The base `compose.yaml` permits outbound HTTPS for real websites, Telegram and Mailchimp Transactional. The automatic `compose.override.yaml` adds Mailpit and makes the application network internal. The explicit monitoring override also uses an internal network. Use the launch commands for the intended network in the [running guide](running.md); changing a configuration filename does not change network isolation. Distinct project names use separate state volumes, but must not run competing pollers with the same Telegram token.

### Local SMTP variables

These values affect only the deliberately invoked `test-email` command. They are separate from the `[email]` incident-delivery configuration above.

| Variable | Default | Accepted value |
| --- | --- | --- |
| `SENTINEL_SMTP_TEST_ENABLED` | Disabled | Exactly `true` enables the local test |
| `SENTINEL_SMTP_HOST` | `mailpit` | Exactly `mailpit`, `localhost`, `127.0.0.1` or `::1` |
| `SENTINEL_SMTP_PORT` | `1025` | Integer `1`–`65535` |
| `SENTINEL_SMTP_TIMEOUT_SECONDS` | `5` | Finite number `0.1`–`10`; bounded overall test deadline |

The local Compose override explicitly sets `true`, `mailpit`, `1025` and `5` in the container. Adding different SMTP values to `.env` alone does not replace those literal Compose settings. Native use requires exported variables and a reachable local sink. Sender and recipient are fixed synthetic `example.test` addresses; there are no configurable recipient, authentication, STARTTLS or production SMTP options. Startup, status and health never send test mail. Enabled `[email]` delivery sends incident notifications independently of this command.

Mailpit's separate service settings live in `compose.override.yaml`. It publishes only its UI on loopback, uses bounded temporary storage, retains at most 100 messages for 24 hours, accepts messages up to 1 MiB for `example.test` recipients only and has no configured relay. Recreating Mailpit clears its inbox. These are sink settings, separate from Sentinel's inventory and Telegram delivery.

## Fixed limits and derived behavior

The following application limits are not configurable TOML or environment fields:

| Area | Current behavior |
| --- | --- |
| Inventory/file bounds | 20 physical aliases, at most 40 expanded polled rules, four filesystems/server, 16 KiB per main/inventory file, 32-character physical aliases, 4,096-byte POST JSON body; upper limits cannot be raised through config |
| Website redirects | GET: at most 3, same origin only. POST: no redirect follow/replay. |
| Durable delivery queue | Telegram: 80 polled website/server plus at most 1,000 incoming pending records. Email: the same separate budgets per recipient, at most 5,400 total; admission failure rolls back the observation/event and all its channel records. In-flight bounds are Telegram 20 and email 5. |
| History | At most 200 terminal records per channel and 200 incident-audit records; not a complete outage archive |
| Telegram send pacing | One FIFO sender; at least 3.1 seconds between incident/reply send starts |
| Incident attempts | At most 8 per destination; backoff with jitter capped at 300 seconds unless the provider requires longer |
| Email send pacing | One sender, at least 1 second between recipient attempts; retries do not block other due recipients |
| Email provider pause | Channel-wide quota/rate hints pause email only; bounded Retry-After up to 7 days is persisted and conservatively repeated on restart |
| Email text/response | Subject at most 200 characters/512 UTF-8 bytes, body at most 16 KiB, provider response at most 16 KiB; plain text without attachments |
| Provider cooldown | Shared by polling-triggered and send-triggered 429s; persisted Retry-After up to 7 days |
| Command rates | 6/minute per user, burst 2; 30/minute globally, burst 5 |
| Command input | At most 256 characters for command parsing; arguments cannot select URLs, recipients or new sources |
| Command reply queue | 20 in-memory replies, at most 3 attempts each; overflow/restart can lose a reply |
| Rendered messages | At most 3,500 UTF-16 units, plain text with control characters removed and previews disabled |
| Telegram transport | At most 20 updates per poll, 20-second long poll, 1 MiB response bound, 10-second ordinary request deadline and 25-second polling request deadline |
| Worker progress | Telegram and email workers must progress within 45 seconds; expected network outages/cooldowns continue updating progress |
| Polling checkpoint | Durable update offset, expired after 24 hours without a handled update |

Current-process scheduling, retries, freshness and reminder deadlines use monotonic time. Stored event and acceptance times use UTC. Restart conservatively repeats saved retry/cooldown durations and an initial send interval. Confirmed incidents persist, pending confirmation series reset and restored observations remain stale until checked again.

For each destination, DOWN must be accepted before its matching RECOVERY. Email reminders use independent accepted-delivery times per recipient and require fresh active DOWN; pushed occurrences have no periodic reminders. A full recipient website queue defers an optional email reminder; an actual state transition or new incoming event still requires atomic capacity for every configured destination. A permanently failed opening blocks its dependent recovery; restarting does not retry that terminal failure. A terminal reminder failure suppresses more reminders for that incident. A later successful send does not erase an older failure from retained history, so status can show accepted records and an older `last_error` together. Provider acceptance is not exactly-once delivery: a crash before local acknowledgement can cause a duplicate.

## Applying changes

Startup atomically upgrades supported schema-1/2/3/4/5 state to schema 6, preserving startup metadata, website incidents, incoming receipts and Telegram/email delivery history. Schema 6 stores normalized server snapshots and independent exporter/resource rule state. Older application versions cannot open schema-6 state. Status/config validation does not initialize or migrate a stopped database; start the matching application first.

Configuration is loaded at process startup; there is no hot reload.

| Change | How to apply | State effect |
| --- | --- | --- |
| Baked TOML configuration or endpoint inventory | Edit the supported repository file, then run `up -d --build --wait` with the same Compose file/project selection | Rebuilds the image and reads its configuration/inventory on startup |
| `.env` credentials, addresses, chat, topic or container inventory selection | Run Compose `up -d --force-recreate --wait`; no image rebuild is needed if baked files are unchanged | Container receives the new environment; `restart` alone preserves its old environment |
| Native TOML or exported environment | Stop and start the native process with the intended values | New startup reads the values |
| Ordinary container restart | Reuses the existing image/environment and the same named state volume | Preserves incidents and delivery history; resets pending confirmations and freshness |
| Site URL, HTTP method, JSON body, marker, accepted status range, either confirmation count, body limit or either request timeout | Apply the inventory change as above | Resets that site's confirmed state to UNKNOWN, cancels obsolete never-attempted records and does not invent RECOVERY; attempted/in-flight outcomes remain protected |
| Poll/reminder interval | Apply the inventory change | Preserves confirmed incident identity; restart begins fresh scheduling/reminder deadlines |
| Site disabled/removed or alias renamed | Apply the inventory change | Removes the old active source state and obsolete never-attempted records; a new alias starts as a new source |
| Server URL, address policy, request timeouts or selected filesystem inventory changed | Apply the inventory change | Invalidates its cached server snapshot; affected rule semantics reset without a fabricated recovery, while protected attempted deliveries remain durable |
| Resource threshold, alert flag, selected mount or confirmation counts changed | Apply the inventory change | Resets only affected rule identities; unchanged rules retain confirmed incidents. No historical alert/recovery is invented for a new rule. |
| Server token rotated | Apply environment and recreate | Preserves source/rule identity and accepted delivery history; credentials are absent from fingerprints and persistence |
| Telegram first enabled | Apply TOML and environment | Binds never-attempted logical-operator records to the configured destination without pretending they were already sent |
| Telegram disabled | Apply TOML | Stops its workers and pauses bound records |
| Telegram chat/topic changed | Apply environment | Old pending records fail with `destination_changed`; old incidents/history are not forwarded to the new destination |
| Email disabled | Apply TOML | Stops its sender and pauses its bound records; other channels continue |
| Email sender/provider/recipient or local sink changed | Apply TOML/environment | Changes the opaque route identity; removed pending routes fail with `destination_changed`, and existing incidents are not forwarded/backfilled to a newly added recipient |
| Email API key or request timeout changed | Apply environment/TOML | Preserves destination identity and pending delivery history; already terminal records are not reset |

Removing a marker is a check-semantics change, so it resets the source rather than proving recovery of its previous incident. Successful sends and terminal failures are not reset merely by rebuilding or restarting. Keep the same project name and volume to retain state; `down` preserves the named volume, while `down --volumes` intentionally deletes it.

Moving identical endpoint tables from inline configuration to `sites_file` preserves their existing GET incident identity. Changing the method or JSON body changes check semantics and resets only that endpoint. Reordering keys in the same JSON object does not change its canonical body or fingerprint.

Validate locally before running:

```bash
docker compose -f compose.yaml run --rm --no-deps sentinel check-config
```

This checks the selected inventory and enabled-channel credentials without sending requests or creating application state. Rebuild first when validating edited baked TOML. For status, health, logs and operational examples, continue with the [running guide](running.md).
