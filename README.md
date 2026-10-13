# Sentinel Bot

A self-hosted Telegram bot for website availability and Linux server monitoring. The intended first release checks configured sources, confirms incidents, and delivers alerts without requiring an application database or a monitoring cluster.

**Status: project planning, 14 October 2026.** The application, Docker image, Compose configuration, and executable tests have not been implemented. Features below describe the target release.

## Planned first release

- Independent HTTP checks with timeouts, TLS verification, latency, and optional content markers.
- Linux CPU, memory, and configured filesystem metrics from existing Node Exporter endpoints.
- Confirmed outage and recovery alerts, reminders, and persistent delivery retries.
- Telegram `/help`, `/status`, `/sites`, `/servers`, and `/server <alias>` commands restricted to a configured destination and user allowlist.
- SQLite state, a Docker runtime, and an offline Compose demo using synthetic sources and a fake Telegram transport.

The bot reads configured sources and writes only its own monitoring state. Server endpoints are provisioned separately; the bot does not run commands or manage services on monitored machines.

## Design direction

Python, asyncio, aiohttp, aiogram, and SQLite in one process. Server metrics come from a Node Exporter adapter; no Prometheus server is required for this design. Dependency versions will be verified and locked during implementation.

The repository and its future demo will be self-contained. Examples and tests will use synthetic data.

Verified setup commands and demo instructions will be added with the implementation. A license has not been selected.
