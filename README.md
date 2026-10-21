# Sentinel Bot

Sentinel Bot is a self-hosted monitoring and notification project for websites, Linux servers, and services. Its purpose is to give operators a clear view of availability and resource usage, and deliver technical alerts through Telegram and email.

The project's functional scope includes:

- Configured HTTP(S) GET and POST availability checks, with independent endpoint intervals, timeouts, TLS verification, response latency, optional JSON request bodies and content validation.
- Direct Node Exporter polling for Linux CPU, memory, uptime and selected filesystems, with independent resource incidents and exporter availability.
- Authenticated HTTP endpoints for events submitted by configured servers and services, including locally detected security incidents.
- Push-only sources that send events without allowing Sentinel to connect to their systems.
- Confirmed outage and resource-threshold alerts, recovery notifications, and reminders for ongoing incidents.
- Telegram status commands for websites and servers, available in a configured group or private chat.
- Access restrictions through an allowed-user list and an optional group topic.
- Persistent monitoring state and a notification queue with retries during delivery failures.
- Independent Telegram and email delivery to operator-configured destinations, with per-recipient retries through Mailchimp Transactional and a local Mailpit incident demo.
- Docker Compose execution with a dedicated state volume and an isolated demonstration using synthetic websites, Linux metrics, authenticated incoming events, a fake Telegram inbox and Mailpit.

The command interface consists of `/help`, `/status`, `/sites`, `/servers`, and `/server <alias>`. Status reports distinguish confirmed incidents from current readings and identify unknown or stale data.

Sources can use polling, event submission, or both. Attack detection belongs to the sending server or service; Sentinel receives its report and routes notifications. Silence from a push-only source does not establish that the source is healthy.

Resource alerts use configurable thresholds, hysteresis and consecutive confirmation. Missing metrics remain unknown; CPU calculations warm up after startup or counter resets. Server commands read cached observations without connecting to a requested address. An isolated synthetic metrics demo delivers incidents into Mailpit without exposing the host filesystem.

Monitoring is read-only and limited to explicitly configured sources. Server exporter endpoints are provisioned separately. The project is independent of business applications and their databases.

[Run the complete offline demonstration](docs/demo.md)

[Configuration, Docker setup and local testing](docs/running.md)

[All supported configuration options](docs/configuration.md)

[Incoming events API: requests, authentication and responses](docs/api.md)
