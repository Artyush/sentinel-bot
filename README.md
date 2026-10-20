# Sentinel Bot

Sentinel Bot is a self-hosted monitoring and notification project for websites, Linux servers, and services. Its purpose is to give operators a clear view of availability and resource usage, and deliver technical alerts through Telegram and email.

The project's functional scope includes:

- HTTP(S) availability checks with timeouts, TLS verification, response latency, and optional content validation.
- Linux CPU, memory, uptime, and configured filesystem metrics from Node Exporter endpoints.
- Authenticated HTTP endpoints for events submitted by configured servers and services, including locally detected security incidents.
- Push-only sources that send events without allowing Sentinel to connect to their systems.
- Confirmed outage and resource-threshold alerts, recovery notifications, and reminders for ongoing incidents.
- Telegram status commands for websites and servers, available in a configured group or private chat.
- Access restrictions through an allowed-user list and an optional group topic.
- Persistent monitoring state and a notification queue with retries during delivery failures.
- Independent Telegram and email delivery to operator-configured destinations.
- Docker Compose execution with a dedicated state volume.

The command interface consists of `/help`, `/status`, `/sites`, `/servers`, and `/server <alias>`. Status reports distinguish confirmed incidents from current readings and identify unknown or stale data.

Sources can use polling, event submission, or both. Attack detection belongs to the sending server or service; Sentinel receives its report and routes notifications. Silence from a push-only source does not establish that the source is healthy.

Monitoring is read-only and limited to explicitly configured sources. Server exporter endpoints are provisioned separately. The project is independent of business applications and their databases.

[Configuration, Docker setup and local testing](docs/running.md)

[All supported configuration options](docs/configuration.md)
