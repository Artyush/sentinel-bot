# Incoming events API

Sentinel accepts technical events from configured servers and services, persists them and queues notifications for every enabled channel and configured destination. Detection belongs to the sender. A push-only source needs no outbound probe from Sentinel.

Linux metrics are collected separately from explicitly configured Node Exporter endpoints. This event route accepts notifications and incident lifecycle reports, not raw metric measurements. A hybrid alias may combine an explicitly polled website or server with incoming events; sender-reported state does not set polling availability or resource readings. See [server configuration](configuration.md#linux-servers-servers).

**`POST /api/v1/events`** — submit one notification or incident lifecycle event.

Use your HTTPS reverse proxy as the base URL. The receiver is optional; enable it using the [running guide](running.md#receive-events-from-servers). Poll-only operation has no HTTP listener. The [OpenAPI specification](openapi.yaml) can be imported into Postman or opened in a compatible Swagger viewer; it is a static file, not a public `/docs` endpoint.

## Authentication

Send `Authorization: Bearer <source-token>` and `Content-Type: application/json`. Each configured source has its own token. Authentication fixes the source identity; a payload cannot select a source, recipient or monitoring target. Tokens authorize event submission only. Never put a token in a URL or commit it.

## Request

Send one JSON object. Unknown fields, duplicate JSON keys and non-finite numbers are rejected.

| Field | Required | Meaning |
| --- | --- | --- |
| `event_id` | Yes | UUID identifying this immutable event; reuse it for retries only. |
| `occurred_at` | Yes | UTC event time, for example `2026-10-20T12:00:00Z`; preserve it on retry. |
| `kind` | Yes | `notification`, `firing` or `resolved`; see below. |
| `type` | Yes | Source-allowed technical identifier, such as `service.health`; up to 64 characters. |
| `severity` | Yes | `info`, `warning` or `critical`. |
| `summary` | Yes | Nonblank single-line plain text, up to 1,000 characters by default. |
| `incident_key` | Lifecycle only | Unique occurrence key, up to 128 characters; required for `firing`/`resolved`, absent for `notification`. |

`type` matches `[a-z][a-z0-9_.-]{0,63}`. An occurrence key matches `[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}`. A new incident needs a new key. UTC timestamps accept `Z` or `+00:00` with up to six fractional second digits; UUIDs and timestamps are normalized before duplicate comparison. Summaries reject control characters and line separators.

| Kind | Behavior |
| --- | --- |
| `notification` | Queue one independent alert immediately. |
| `firing` | Open an occurrence and queue its first alert; repeated firing reports do not send another opening. |
| `resolved` | Resolve the matching occurrence using its opening destinations. Resolution received first stores a terminal record without a fabricated recovery. |

The first accepted lifecycle report fixes its `type`. A resolved occurrence cannot reopen while its terminal record is retained, including when a newer firing report arrives. A resolution of an active occurrence cannot predate its opening. Silence leaves source availability unknown and does not resolve an incident.

## Example

Save the following as `event.json`, replacing the UUID and timestamp with values for the actual event. The configured source must allow `service.health`.

```json
{
  "event_id": "4a1e13b2-8b9a-40ae-bcad-8ebdf0fc1793",
  "occurred_at": "2026-10-20T12:00:00Z",
  "kind": "firing",
  "type": "service.health",
  "severity": "critical",
  "summary": "Worker stopped responding.",
  "incident_key": "worker-7f829032"
}
```

```bash
curl --fail-with-body --max-time 10 \
  -H "Authorization: Bearer ${SENTINEL_SOURCE_APP_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data-binary @event.json \
  https://sentinel.example.test/api/v1/events
```

To resolve it, submit a new `event_id`, a later `occurred_at`, `kind: "resolved"` and the same `type`/`incident_key`. For a one-shot notification use `kind: "notification"` and omit `incident_key`. The OpenAPI file contains all three request examples. Export the token locally before using curl; Compose's `.env` does not export shell variables.

## Responses and retries

`202 Accepted` is returned only after the receipt, lifecycle decision and required outbox records commit together:

```json
{
  "event_id": "4a1e13b2-8b9a-40ae-bcad-8ebdf0fc1793",
  "received_at": "2026-10-20T12:00:01+00:00",
  "outcome": "opened"
}
```

Outcomes are `notification`, `opened`, `resolved` or `noop`. Acceptance does not mean a notification provider accepted the message or a person received it. A retry with the same authenticated source, ID and canonical content returns the original receipt; changed content returns `409`. Retained identical retries are checked before new-event age and durable admission-capacity limits; request rate and concurrency limits still apply. A `noop` can still be durably accepted, for example a resolution received before its firing.

Application errors have the form `{"error":{"code":"unauthorized"}}`; they contain no tokens, raw bodies or private transport errors. The HTTP parser or reverse proxy may reject malformed protocol framing before dispatch, with a response outside this JSON contract.

| Status | Meaning | Sender action |
| --- | --- | --- |
| `400` | Invalid JSON/schema or query parameters | Correct the request. |
| `401` | Missing/invalid source token | Correct source authentication. |
| `403` | Event type not allowed for this source | Check source configuration. |
| `404` / `405` | Unknown route / wrong HTTP method | Use the documented POST route. |
| `408` | Request body read deadline exceeded | Retry the immutable event. |
| `409` | Existing ID has different canonical content | Recover the original event; do not overwrite it. |
| `413` / `415` | Body too large / unsupported media type or encoding | Correct the body or headers. |
| `422` | Invalid event time or lifecycle transition | Inspect timestamps, occurrence key and type. |
| `429` | Request rate exhausted | Honor `Retry-After`. |
| `503` | Admission/storage temporarily unavailable | Honor `Retry-After` and retry later. |

After connection loss, timeout, `408`, `429` or `5xx`, retain the event locally and retry with bounded backoff and the same ID/content. A response may be lost after commit, so never create a fresh ID merely because an acknowledgement was lost. Permanent rejection or expiry requires visible sender-side handling. External provider acceptance followed by a local crash can still duplicate a Telegram message or email; recipients already marked provider-accepted are not deliberately retried.

## Default limits

The JSON body is at most 16 KiB, read within 5 seconds, with 10 concurrent requests. Rates are 60/minute per source (burst 10) and 300/minute globally (burst 50). New events may be at most 24 hours old or 5 minutes ahead. Receipts and resolved occurrence records are retained for at least 48 hours, subject to bounded admission; protected records are not evicted to admit new events. Beyond retention, old event times are rejected and senders must never reuse occurrence keys. See the [configuration reference](configuration.md#incoming-events) for configurable bounds and queue budgets.
