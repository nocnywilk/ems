import asyncio
import structlog
from datetime import datetime, timezone

log = structlog.get_logger()

class ConfirmLoop:
    def __init__(self, settings, influx_client, actuation_loop):
        self.settings = settings
        self.influx = influx_client
        self.actuation_loop = actuation_loop
        self.interval = settings.ems.confirm_interval_sec

    async def run_forever(self):
        log.info("confirm_loop.start", interval=self.interval)
        while True:
            try:
                await self._tick()
            except Exception:
                log.exception("confirm_loop.error")
            await asyncio.sleep(self.interval)

    async def _tick(self):
        now = datetime.now(timezone.utc)
        expected = self.actuation_loop._last_sent_command
        log.info("confirm_loop.tick", ts=now.isoformat(), expected=expected)
