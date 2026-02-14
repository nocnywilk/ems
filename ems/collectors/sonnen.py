import httpx
import structlog
from datetime import datetime, timezone
from influxdb_client.client.write.point import Point
from .base import BaseCollector

log = structlog.get_logger()


class SonnenCollector(BaseCollector):
    """
    Collects data from Sonnenbatterie local API.

    Endpoint: http://<IP>/api/v2/status
    Auth: Bearer token in header
    Interval: 30s (battery state changes quickly)

    Writes to 'telemetry' bucket:
        measurement: battery
        fields: soc, production_w, consumption_w, grid_feed_in_w,
                grid_purchase_w, battery_charging_w, battery_discharging_w,
                pac_total_w
        tags: source=sonnen
    """

    name = "sonnen"
    interval_sec = 30

    def __init__(self, settings, influx_client):
        super().__init__(settings, influx_client)
        self._base_url = f"http://{settings.sonnen.ip}/api/v2"
        self._headers = {"Auth-Token": settings.sonnen.token}
        self._client = httpx.AsyncClient(timeout=10.0)

    async def _collect(self):
        resp = await self._client.get(
            f"{self._base_url}/status",
            headers=self._headers,
        )
        resp.raise_for_status()
        data = resp.json()

        now = datetime.now(timezone.utc)

        # Core battery metrics
        soc = data.get("USOC", 0)  # User State of Charge (%)
        production_w = data.get("Production_W", 0)
        consumption_w = data.get("Consumption_W", 0)
        grid_feed_in_w = data.get("GridFeedIn_W", 0)  # positive = export
        grid_purchase_w = data.get("GridPurchase_W", 0)  # positive = import (added by Sonnen, not in older FW)
        pac_total_w = data.get("Pac_total_W", 0)  # positive = discharge, negative = charge
        battery_charging_w = max(0, -pac_total_w)
        battery_discharging_w = max(0, pac_total_w)

        # Operating mode and system status
        system_status = data.get("SystemStatus", "unknown")
        battery_activity = "idle"
        if battery_charging_w > 50:
            battery_activity = "charging"
        elif battery_discharging_w > 50:
            battery_activity = "discharging"

        point = (
            Point("battery")
            .tag("source", "sonnen")
            .field("soc", float(soc))
            .field("production_w", float(production_w))
            .field("consumption_w", float(consumption_w))
            .field("grid_feed_in_w", float(grid_feed_in_w))
            .field("grid_purchase_w", float(grid_purchase_w))
            .field("pac_total_w", float(pac_total_w))
            .field("battery_charging_w", float(battery_charging_w))
            .field("battery_discharging_w", float(battery_discharging_w))
            .field("system_status", system_status)
            .field("battery_activity", battery_activity)
            .time(now)
        )

        await self._write_points([point], bucket="telemetry")

        log.info(
            "sonnen.collected",
            soc=soc,
            production_w=production_w,
            consumption_w=consumption_w,
            pac_total_w=pac_total_w,
            activity=battery_activity,
        )
