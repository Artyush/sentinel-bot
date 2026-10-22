# Complete offline demonstration

Run the real Sentinel application with synthetic websites, Node Exporter readings and authenticated push events. A fake Telegram inbox and Mailpit receive its notifications. No bot, paid provider, private server or local `.env` is required. All metrics are synthetic; the containers do not inspect your machine.

Docker Engine and the Compose plugin must be running. The first build downloads pinned public images and Python dependencies. After building, the application network is internal; the demonstration makes no requests to real Telegram, Mailchimp or public websites. Run the following commands from the cloned repository root.

## Start

```bash
docker compose --env-file .env.example -p sentinel-release-demo -f compose.yaml -f compose.override.yaml -f compose.monitoring.yaml -f compose.metrics.yaml -f compose.release.yaml up -d --build --wait
```

The override selects [release.local.toml](../config/release.local.toml), supplies fixed synthetic channel/source settings and clears live credentials. It adds the fake Telegram service to the existing website, metrics and Mailpit fixtures. The receiver and fixture controls remain private to Compose; only the two inbox interfaces are published, both on loopback:

- [Fake Telegram inbox](http://127.0.0.1:8026).
- [Mailpit inbox](http://127.0.0.1:8025).

If these ports are already occupied, export `TELEGRAM_DEMO_HTTP_PORT=18026` and `MAILPIT_HTTP_PORT=18025` before starting; open those ports instead. Keep the same project name and Compose files for every command. The following shell function saves that selection for the rest of this guide:

```bash
sentdemo() {
  docker compose --env-file .env.example -p sentinel-release-demo \
    -f compose.yaml -f compose.override.yaml -f compose.monitoring.yaml \
    -f compose.metrics.yaml -f compose.release.yaml "$@"
}
```

Inspect configuration, worker health and cached status:

```bash
sentdemo ps
sentdemo exec -T sentinel python -m sentinel_bot check-config
sentdemo exec -T sentinel python -m sentinel_bot healthcheck
sentdemo exec -T sentinel python -m sentinel_bot status
```

The configured sources are:

| Alias | Input | Initial behavior |
| --- | --- | --- |
| `primary` | Synthetic website GET | Becomes UP after successful checks. |
| `secondary` | Synthetic website POST | Independent interval and incident state; the repeated request has no business action. |
| `node` | Synthetic exporter on port 9100 | Availability and memory alerts; CPU, uptime and filesystem values are also displayed. |
| `peer` | Synthetic exporter on port 9101 | Independent informational resource readings and exporter availability. |
| `edge` | Authenticated push-only events | Availability remains unknown; Sentinel never probes this source. |

CPU needs two valid scrapes to establish a delta. Startup, health, status and initial UP do not send a greeting or an alert. Wait for healthy cached readings before starting a scenario. Telegram sends are paced at least 3.1 seconds apart; email sends are paced independently. Confirmation and delivery take time, so inspect status rather than expecting an immediate inbox change.

## Website outage and recovery

```bash
sentdemo exec -T telegram-fixture python /app/demo/control.py site primary down
sentdemo exec -T sentinel python -m sentinel_bot status
```

After three failed checks, `primary` is DOWN with one incident ID. Its DOWN alert appears in the fake Telegram inbox and once for each of the two synthetic Mailpit recipients. `secondary` and both exporters continue independently. Recover the source:

```bash
sentdemo exec -T telegram-fixture python /app/demo/control.py site primary up
```

Two successful checks close the incident. Each destination receives RECOVERY after its opening alert. Use `site primary slow` to exercise a request timeout while the other sources continue.

## Linux resource incident

```bash
sentdemo exec -T telegram-fixture python /app/demo/control.py metrics memory-high
```

Three high memory readings open `node/memory`; the exporter itself remains available. Cached status distinguishes the resource incident from exporter availability. Restore the normal synthetic values for two-reading recovery:

```bash
sentdemo exec -T telegram-fixture python /app/demo/control.py metrics up
```

This example alerts on memory only. Real inventories can enable separate CPU and selected-filesystem rules; see the [configuration reference](configuration.md#linux-servers-servers). No host mounts, privileges, Docker socket or host namespaces are needed by the bot.

## Authenticated push event and duplicate

The helper submits an event to Sentinel's private receiver using the configured synthetic source token. It generates the UUID and UTC timestamp and saves the immutable request before sending. `duplicate` replays the most recently saved event, including a resolution if that was the latest submission. This temporary sender file survives a Sentinel restart but is lost when the Telegram fixture is recreated:

```bash
sentdemo exec -T telegram-fixture python /app/demo/control.py event firing --key demo-incident
sentdemo exec -T telegram-fixture python /app/demo/control.py event duplicate
```

The first request returns a durable `202` receipt with `outcome: "opened"`; the exact duplicate returns that original receipt and creates no second opening. The `edge` source sends to both notification channels without creating any outbound probe or changing website/server availability. Resolve the occurrence:

```bash
sentdemo exec -T telegram-fixture python /app/demo/control.py event resolved --key demo-incident
```

Resolution uses a new event UUID and the same occurrence key. It closes the sender-reported incident and queues the ordered recovery. Use a new `--key` for every new occurrence; a retained resolved key cannot reopen. For an independent one-shot message, run `event notification`. The [API guide](api.md) documents authentication, payloads, durable receipts and retry rules for a real sender.

## Channel failure and retry

Temporarily make Telegram's send endpoint return HTTP 503, then open a new pushed occurrence:

```bash
sentdemo exec -T telegram-fixture python /app/demo/control.py telegram down
sentdemo exec -T telegram-fixture python /app/demo/control.py event firing --key telegram-outage
sentdemo exec -T sentinel python -m sentinel_bot status
```

Mailpit still receives both recipient messages, polling continues and worker health stays healthy. Telegram status shows pending/retrying delivery rather than accepted delivery. Restore the fake provider promptly, before its bounded eight-attempt budget is exhausted:

```bash
sentdemo exec -T telegram-fixture python /app/demo/control.py telegram up
```

Telegram retries the durable opening after its scheduled delay; email recipients already marked accepted are not deliberately resent. Resolve with `event resolved --key telegram-outage` after the opening is accepted. The fixture models provider acceptance; it does not send to a real Telegram account or establish production inbox delivery.

## Cached commands and restart

Submit an authorized synthetic command through the fake provider:

```bash
sentdemo exec -T telegram-fixture python /app/demo/control.py command '/servers'
sentdemo exec -T telegram-fixture python /app/demo/control.py command '/status'
```

Replies appear in the fake Telegram inbox and use the shared sender pacing. Commands read persisted observations; they do not trigger extra probes or discover hosts.

To observe restart continuity, open another website incident, wait until status records accepted Telegram/email delivery, and note its incident ID:

```bash
sentdemo exec -T telegram-fixture python /app/demo/control.py site primary down
sentdemo exec -T sentinel python -m sentinel_bot status
sentdemo restart sentinel
sentdemo exec -T sentinel python -m sentinel_bot status
```

The confirmed incident ID and accepted delivery records survive. Restored readings remain stale until new checks; CPU warms up again. An incident already durably accepted by a destination is not deliberately sent there again. A crash after external acceptance but before SQLite acknowledgement can still produce a duplicate; exactly-once external delivery is not promised. Restore `primary` with `site primary up` when finished.

## Logs, cleanup and repeatable checks

```bash
sentdemo logs --tail=80 sentinel
sentdemo down
```

`down` removes this project's containers and networks while preserving its named Sentinel state volume. Both fixture inboxes are temporary and reset when their containers are recreated; SQLite delivery history remains. Fake Telegram keeps the latest 200 accepted messages, while Mailpit retains at most 100 messages for 24 hours. Fixtures restart in their normal state, so bringing the stack back can legitimately recover an incident that was still open. For a fresh demonstration, choose a different project name in both startup and `sentdemo`, and free or change the inbox ports.

Run the automated combined scenario from the repository root:

```bash
python3 scripts/release_smoke.py
```

It uses a disposable project and removes only its own containers, network and state volume. The broader [development checks](running.md#native-development-and-checks) cover the individual boundaries. This demonstration does not provision a real exporter, HTTPS reverse proxy, Telegram bot, Mailchimp account or production deployment.
