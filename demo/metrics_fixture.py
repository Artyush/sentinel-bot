"""Synthetic Node Exporter exposition; never reads the host or container metrics."""

from __future__ import annotations

import asyncio
import signal
import time
from dataclasses import dataclass, field

from aiohttp import web


@dataclass
class SyntheticNode:
    state: str = "up"
    user_seconds: float = 100.0
    idle_seconds: float = 400.0
    observed_at: float = field(default_factory=time.monotonic)
    boot_time: float = field(default_factory=lambda: time.time() - 86400)

    def advance(self) -> None:
        now = time.monotonic()
        elapsed = now - self.observed_at
        busy = 0.95 if self.state in {"high", "cpu-high", "missing"} else 0.2
        if self.state == "between":
            busy = 0.8
        self.user_seconds += elapsed * busy
        self.idle_seconds += elapsed * (1 - busy)
        self.observed_at = now

    def exposition(self) -> str:
        self.advance()
        memory = 92 if self.state in {"high", "memory-high", "missing"} else 50
        filesystem = 92 if self.state in {"high", "filesystem-high"} else 50
        if self.state == "between":
            memory = filesystem = 80
        lines = [
            "# HELP node_cpu_seconds_total Synthetic CPU counter, not host usage.",
            "# TYPE node_cpu_seconds_total counter",
        ]
        for cpu in ("0", "1"):
            for mode, value in (
                ("user", self.user_seconds),
                ("idle", self.idle_seconds),
                ("system", 10),
                ("nice", 1),
                ("iowait", 1),
                ("irq", 0),
                ("softirq", 0),
                ("steal", 0),
                ("guest", 0),
                ("guest_nice", 0),
            ):
                lines.append(f'node_cpu_seconds_total{{cpu="{cpu}",mode="{mode}"}} {value}')
        if self.state != "missing":
            lines.extend(
                (
                    "node_memory_MemTotal_bytes 1000000",
                    f"node_memory_MemAvailable_bytes {(100 - memory) * 10000}",
                )
            )
        labels = 'device="/dev/synthetic",fstype="ext4",mountpoint="/"'
        lines.extend(
            (
                f"node_filesystem_size_bytes{{{labels}}} 1000000",
                f"node_filesystem_avail_bytes{{{labels}}} {(100 - filesystem) * 10000}",
                f"node_filesystem_readonly{{{labels}}} 0",
                f"node_filesystem_device_error{{{labels}}} 0",
                f"node_boot_time_seconds {self.boot_time}",
            )
        )
        for collector in ("cpu", "meminfo", "filesystem", "stat"):
            lines.append(f'node_scrape_collector_success{{collector="{collector}"}} 1')
        return "\n".join(lines) + "\n"


async def application(node: SyntheticNode) -> web.Application:
    capacity = asyncio.Semaphore(8)

    async def metrics(request: web.Request) -> web.Response:
        if capacity.locked():
            raise web.HTTPServiceUnavailable
        async with capacity:
            if node.state == "slow":
                await asyncio.sleep(3)
            if node.state == "down":
                return web.Response(status=503, text="Synthetic exporter outage")
            return web.Response(
                text=node.exposition(),
                content_type="text/plain",
                headers={"X-Sentinel-Fixture": "synthetic"},
            )

    async def control(request: web.Request) -> web.Response:
        state = await request.text()
        if state not in {
            "up",
            "high",
            "between",
            "cpu-high",
            "memory-high",
            "filesystem-high",
            "missing",
            "reset",
            "down",
            "slow",
        }:
            raise web.HTTPBadRequest
        node.advance()
        if state == "reset":
            node.user_seconds = node.idle_seconds = 0
            node.state = "up"
        else:
            node.state = state
        return web.Response(text=state)

    async def health(request: web.Request) -> web.Response:
        return web.Response(text="SYNTHETIC")

    app = web.Application(client_max_size=32)
    app.router.add_get("/metrics", metrics)
    app.router.add_get("/health", health)
    app.router.add_post("/control", control)
    return app


async def main() -> None:
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    runners: list[web.AppRunner] = []
    try:
        for port in (9100, 9101):
            runner = web.AppRunner(
                await application(SyntheticNode()),
                access_log=None,
                handler_cancellation=True,
                shutdown_timeout=1,
                max_headers=32,
                max_line_size=2048,
                max_field_size=1024,
                keepalive_timeout=2,
            )
            await runner.setup()
            runners.append(runner)
            await web.TCPSite(runner, "0.0.0.0", port).start()
        await stopped.wait()
    finally:
        for runner in reversed(runners):
            await runner.cleanup()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    asyncio.run(main())
