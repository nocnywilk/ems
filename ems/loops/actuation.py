import asyncio
import structlog

log = structlog.get_logger()

class ActuationLoop:
    def __init__(self, settings, influx_client, decision_loop):
        self.settings = settings
        self.influx = influx_client
        self.decision_loop = decision_loop
        self.dry_run = settings.ems.dry_run
        self._last_sent_command = None

    async def run_forever(self):
        log.info("actuation_loop.start", dry_run=self.dry_run)
        while True:
            try:
                await self._tick()
            except Exception:
                log.exception("actuation_loop.error")
            await asyncio.sleep(10)

    async def _tick(self):
        decision = self.decision_loop.current_decision
        if decision == self._last_sent_command:
            return
        log.info("actuation_loop.new_command", decision=decision, previous=self._last_sent_command, dry_run=self.dry_run)
        if self.dry_run:
            log.info("actuation_loop.dry_run_skip", decision=decision)
        self._last_sent_command = decision
