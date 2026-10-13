# Sentinel Bot

Sentinel Bot is a self-hosted Telegram monitoring bot project for websites and Linux servers. Its purpose is to give operators a clear view of website availability and server resource usage, with technical alerts about problems and recovery.

The project's functional scope includes:

- HTTP(S) availability checks with timeouts, TLS verification, response latency, and optional content validation.
- Linux CPU, memory, uptime, and configured filesystem metrics from Node Exporter endpoints.
- Confirmed outage and resource-threshold alerts, recovery notifications, and reminders for ongoing incidents.
- Telegram status commands for websites and servers, available in a configured group or private chat.
- Access restrictions through an allowed-user list and an optional group topic.
- Persistent monitoring state and a notification queue with retries during delivery failures.
- Docker Compose execution with a dedicated state volume.

The command interface consists of `/help`, `/status`, `/sites`, `/servers`, and `/server <alias>`. Status reports distinguish confirmed incidents from current readings and identify unknown or stale data.

Monitoring is read-only and limited to explicitly configured sources. Server exporter endpoints are provisioned separately. The project is independent of business applications and their databases.
