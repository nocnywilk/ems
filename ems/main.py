import asyncio
import logging
import os
import sys
from pathlib import Path
import structlog
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from config import load_settings
from loops import DecisionLoop, ActuationLoop
from collectors import TibberCollector

log_level = os.getenv("EMS_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, log_level, logging.INFO))

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer() if log_level == "DEBUG" else structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
)
log = structlog.get_logger()
HEARTBEAT_PATH = Path("/var/log/ems/heartbeat")

async def heartbeat():
    HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
    while True:
        HEARTBEAT_PATH.touch()
        await asyncio.sleep(30)

async def main():
    log.info("ems.starting", version="0.1.0")
    try:
        settings = load_settings()
    except Exception as e:
        log.error("ems.config_error", error=str(e))
        sys.exit(1)
    # Test-mode overrides requested by user.
    settings.ems.decision_interval_sec = 60
    settings.ems.dry_run = True
    log.info(
        "ems.config_loaded",
        dry_run=settings.ems.dry_run,
        decision_interval=settings.ems.decision_interval_sec,
        influx_url=settings.influx.url,
    )
    influx = InfluxDBClientAsync(url=settings.influx.url, token=settings.influx.token, org=settings.influx.org)
    ready = await influx.ping()
    if not ready:
        log.error("ems.influx_not_ready")
        sys.exit(1)
    log.info("ems.influx_connected")
    decision = DecisionLoop(settings, influx)
    decision.interval = 60
    actuation = ActuationLoop(settings, influx, decision)
    tibber = TibberCollector(settings, influx)

    log.info("ems.loops_starting")
    try:
        await asyncio.gather(
            heartbeat(),
            # Control loops
            decision.run_forever(),
            actuation.run_forever(),
            # Data collectors
            tibber.run_forever(),
        )
    except asyncio.CancelledError:
        log.info("ems.shutdown")
    finally:
        await influx.close()
        log.info("ems.stopped")

if __name__ == "__main__":
    asyncio.run(main())
