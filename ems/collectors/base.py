import asyncio
import structlog
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write.point import Point

log = structlog.get_logger()


class BaseCollector(ABC):
    """Base class for all data collectors."""

    name: str = "base"
    interval_sec: int = 60

    def __init__(self, settings, influx_client: InfluxDBClientAsync):
        self.settings = settings
        self.influx = influx_client
        self._consecutive_errors = 0
        self._max_backoff = 300  # 5 min max backoff

    async def run_forever(self):
        log.info(f"collector.start", collector=self.name, interval=self.interval_sec)
        # Initial delay to let the system stabilize
        await asyncio.sleep(2)
        while True:
            try:
                await self._collect()
                self._consecutive_errors = 0
            except Exception:
                self._consecutive_errors += 1
                backoff = min(
                    self.interval_sec * (2 ** (self._consecutive_errors - 1)),
                    self._max_backoff,
                )
                log.exception(
                    "collector.error",
                    collector=self.name,
                    consecutive_errors=self._consecutive_errors,
                    backoff_sec=backoff,
                )
                await asyncio.sleep(backoff)
                continue
            await asyncio.sleep(self.interval_sec)

    @abstractmethod
    async def _collect(self):
        """Override: fetch data and call self._write_points()."""
        ...

    async def _write_points(self, points: list[Point], bucket: str):
        """Write a batch of InfluxDB points."""
        if not points:
            return
        write_api = self.influx.write_api()
        await write_api.write(
            bucket=bucket,
            org=self.settings.influx.org,
            record=points,
        )
        log.info(
            "collector.write",
            collector=self.name,
            bucket=bucket,
            points=len(points),
        )
